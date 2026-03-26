package com.cassandratool.ui;

import com.cassandratool.model.ConnectionProfile;
import com.cassandratool.service.CassandraBulkLoadService;
import com.cassandratool.service.CassandraClusterDiagnostics;
import com.cassandratool.service.CassandraSchemaService;
import com.cassandratool.service.CassandraSelectService;
import com.cassandratool.service.CassandraSessionFactory;
import com.cassandratool.service.ConnectionStore;
import com.cassandratool.service.CsvParseService;
import com.cassandratool.service.CsvTableData;
import com.cassandratool.service.CqlIdentifiers;
import com.datastax.oss.driver.api.core.CqlSession;
import com.opencsv.exceptions.CsvException;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListView;
import javafx.scene.control.PasswordField;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.Spinner;
import javafx.scene.control.SpinnerValueFactory;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.stage.FileChooser;
import javafx.stage.Window;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainController implements Initializable {

    private static final int PREVIEW_ROW_CAP = 200;

    @FXML
    private ComboBox<ConnectionProfile> profileCombo;
    @FXML
    private TextField profileNameField;
    @FXML
    private TextField contactPointsField;
    @FXML
    private TextField portField;
    @FXML
    private TextField datacenterField;
    @FXML
    private TextField usernameField;
    @FXML
    private PasswordField passwordField;
    @FXML
    private Button saveProfileButton;
    @FXML
    private Button connectButton;
    @FXML
    private Button disconnectButton;
    @FXML
    private Label connectionStatusLabel;
    @FXML
    private ListView<String> filesListView;
    @FXML
    private ComboBox<String> keyspaceCombo;
    @FXML
    private ComboBox<String> tableCombo;
    @FXML
    private Spinner<Integer> recordsSpinner;
    @FXML
    private CheckBox firstRowIsHeaderCheck;
    @FXML
    private Button selectFromDbButton;
    @FXML
    private TableView<ObservableList<String>> dbDataTable;
    @FXML
    private Button previewButton;
    @FXML
    private Button loadButton;
    @FXML
    private TableView<ObservableList<String>> previewTable;
    @FXML
    private ProgressBar loadProgressBar;
    @FXML
    private Label progressDetailLabel;
    @FXML
    private Label countersLabel;
    @FXML
    private TextArea logArea;

    private final ConnectionStore connectionStore = new ConnectionStore();
    private final ObservableList<ConnectionProfile> profiles = FXCollections.observableArrayList();
    private final ObservableList<String> filePaths = FXCollections.observableArrayList();
    private final CsvParseService csvParseService = new CsvParseService();
    private final CassandraBulkLoadService bulkLoadService = new CassandraBulkLoadService();
    private final CassandraSelectService selectService = new CassandraSelectService();
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "cassandra-csv-loader");
        t.setDaemon(true);
        return t;
    });

    private CqlSession session;
    private int lastParsedRowCount;
    private boolean schemaListenersMuted;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        filesListView.setItems(filePaths);
        profileCombo.setItems(profiles);
        profileCombo.setCellFactory(lv -> new javafx.scene.control.ListCell<>() {
            @Override
            protected void updateItem(ConnectionProfile item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty || item == null ? null : item.toString());
            }
        });
        profileCombo.setButtonCell(new javafx.scene.control.ListCell<>() {
            @Override
            protected void updateItem(ConnectionProfile item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty || item == null ? null : item.toString());
            }
        });
        profileCombo.getSelectionModel().selectedItemProperty().addListener((obs, o, n) -> {
            if (n != null) {
                fillFormFromProfile(n);
            }
        });

        SpinnerValueFactory.IntegerSpinnerValueFactory recordsFactory =
                new SpinnerValueFactory.IntegerSpinnerValueFactory(1, 1_000_000, 100);
        recordsSpinner.setValueFactory(recordsFactory);
        recordsSpinner.setEditable(true);
        recordsSpinner.focusedProperty().addListener((obs, was, isNow) -> {
            if (!isNow) {
                commitSpinner(recordsSpinner);
            }
        });

        reloadProfilesFromDisk();
        countersLabel.setText("Загружено: 0 | Осталось: — | Всего в плане: —");
        progressDetailLabel.setText("—");
        loadProgressBar.setProgress(0);
        selectFromDbButton.setDisable(true);

        keyspaceCombo.setEditable(true);
        tableCombo.setEditable(true);
        keyspaceCombo.setOnAction(e -> {
            if (!schemaListenersMuted) {
                loadTablesForKeyspaceAsync(comboText(keyspaceCombo));
            }
        });

        Platform.runLater(() -> {
            Window w = connectionStatusLabel.getScene().getWindow();
            w.setOnCloseRequest(e -> shutdown());
        });
    }

    private static void commitSpinner(Spinner<Integer> spinner) {
        try {
            String text = spinner.getEditor().getText();
            SpinnerValueFactory.IntegerSpinnerValueFactory f =
                    (SpinnerValueFactory.IntegerSpinnerValueFactory) spinner.getValueFactory();
            int v = Integer.parseInt(text.trim());
            int min = f.getMin();
            int max = f.getMax();
            v = Math.max(min, Math.min(max, v));
            f.setValue(v);
        } catch (NumberFormatException ignored) {
            spinner.getValueFactory().setValue(spinner.getValueFactory().getValue());
        }
    }

    private void reloadProfilesFromDisk() {
        try {
            profiles.setAll(connectionStore.loadAll());
        } catch (IOException e) {
            appendLog("Ошибка чтения сохранённых подключений: " + e.getMessage());
        }
    }

    private void fillFormFromProfile(ConnectionProfile p) {
        profileNameField.setText(blankToEmpty(p.getDisplayName()));
        contactPointsField.setText(blankToEmpty(p.getContactPoints()));
        portField.setText(p.getPort() > 0 ? String.valueOf(p.getPort()) : "9042");
        datacenterField.setText(blankToEmpty(p.getLocalDatacenter()));
        usernameField.setText(blankToEmpty(p.getUsername()));
        passwordField.setText(p.getPassword() != null ? p.getPassword() : "");
    }

    private static String blankToEmpty(String s) {
        return s == null ? "" : s;
    }

    private ConnectionProfile profileFromForm() {
        ConnectionProfile sel = profileCombo.getSelectionModel().getSelectedItem();
        String id = sel != null ? sel.getId() : UUID.randomUUID().toString();
        int port = 9042;
        try {
            port = Integer.parseInt(portField.getText().trim());
        } catch (NumberFormatException e) {
            port = 9042;
        }
        return new ConnectionProfile(
                id,
                profileNameField.getText().trim(),
                contactPointsField.getText().trim(),
                port,
                datacenterField.getText().trim(),
                usernameField.getText().trim(),
                passwordField.getText());
    }

    @FXML
    private void onSaveProfile() {
        ConnectionProfile p = profileFromForm();
        List<String> missing = CassandraSessionFactory.validateProfile(p);
        if (!missing.isEmpty()) {
            alert(Alert.AlertType.WARNING, "Заполните поля: " + String.join(", ", missing));
            return;
        }
        if (p.getDisplayName() == null || p.getDisplayName().isBlank()) {
            alert(Alert.AlertType.WARNING, "Укажите имя профиля.");
            return;
        }
        boolean updated = false;
        for (int i = 0; i < profiles.size(); i++) {
            if (profiles.get(i).getId().equals(p.getId())) {
                profiles.set(i, p);
                updated = true;
                break;
            }
        }
        if (!updated) {
            profiles.add(p);
        }
        try {
            connectionStore.saveAll(new ArrayList<>(profiles));
            profileCombo.getSelectionModel().select(p);
            appendLog("Подключение сохранено: " + p.getDisplayName());
        } catch (IOException e) {
            alert(Alert.AlertType.ERROR, "Не удалось сохранить: " + e.getMessage());
        }
    }

    @FXML
    private void onConnect() {
        commitSpinner(recordsSpinner);
        ConnectionProfile p = profileFromForm();
        List<String> missing = CassandraSessionFactory.validateProfile(p);
        if (!missing.isEmpty()) {
            alert(Alert.AlertType.WARNING, "Заполните поля: " + String.join(", ", missing));
            return;
        }
        connectButton.setDisable(true);
        Task<CqlSession> task = new Task<>() {
            @Override
            protected CqlSession call() {
                return CassandraSessionFactory.connect(p);
            }
        };
        task.setOnSucceeded(ev -> {
            closeSessionQuietly();
            session = task.getValue();
            connectButton.setDisable(false);
            disconnectButton.setDisable(false);
            connectionStatusLabel.setText("Подключено: " + p.getContactPoints());
            appendLog("Сессия Cassandra открыта.");
            CassandraClusterDiagnostics.logTopology(session, MainController.this::appendLog);
            CassandraClusterDiagnostics.warnIfLocalDcMismatch(session, p.getLocalDatacenter(), MainController.this::appendLog);
            selectFromDbButton.setDisable(false);
            refreshKeyspacesFromClusterAsync();
        });
        task.setOnFailed(ev -> {
            connectButton.setDisable(false);
            Throwable ex = task.getException();
            String msg = ex != null ? ex.getMessage() : "неизвестная ошибка";
            connectionStatusLabel.setText("Ошибка подключения");
            appendLog("Ошибка подключения: " + msg);
            appendLog("Подсказка: после удачного подключения смотрите в лог строку datacenter= у узла — в «Local DC» нужно то же имя.");
            alert(Alert.AlertType.ERROR, "Не удалось подключиться:\n" + msg);
        });
        executor.submit(task);
    }

    @FXML
    private void onDisconnect() {
        closeSessionQuietly();
        disconnectButton.setDisable(true);
        connectionStatusLabel.setText("Не подключено");
        appendLog("Сессия закрыта.");
        selectFromDbButton.setDisable(true);
        dbDataTable.getItems().clear();
        dbDataTable.getColumns().clear();
        clearSchemaCombos();
    }

    private static String comboText(ComboBox<String> combo) {
        if (combo.getEditor() != null && combo.getEditor().getText() != null) {
            String t = combo.getEditor().getText().trim();
            if (!t.isEmpty()) {
                return t;
            }
        }
        String v = combo.getValue();
        return v != null ? v.trim() : "";
    }

    private void clearSchemaCombos() {
        schemaListenersMuted = true;
        keyspaceCombo.getItems().clear();
        tableCombo.getItems().clear();
        keyspaceCombo.setValue(null);
        tableCombo.setValue(null);
        if (keyspaceCombo.getEditor() != null) {
            keyspaceCombo.getEditor().setText("");
        }
        if (tableCombo.getEditor() != null) {
            tableCombo.getEditor().setText("");
        }
        schemaListenersMuted = false;
    }

    private void refreshKeyspacesFromClusterAsync() {
        if (session == null) {
            return;
        }
        Task<List<String>> task = new Task<>() {
            @Override
            protected List<String> call() {
                return CassandraSchemaService.listKeyspaces(session);
            }
        };
        task.setOnSucceeded(ev -> {
            schemaListenersMuted = true;
            keyspaceCombo.getItems().setAll(task.getValue());
            schemaListenersMuted = false;
            appendLog("Загружены keyspace из кластера: " + task.getValue().size() + " шт.");
            loadTablesForKeyspaceAsync(comboText(keyspaceCombo));
        });
        task.setOnFailed(ev -> appendLog("Не удалось загрузить keyspace: "
                + (task.getException() != null ? task.getException().getMessage() : "")));
        executor.submit(task);
    }

    private void loadTablesForKeyspaceAsync(String keyspace) {
        if (session == null) {
            Platform.runLater(() -> tableCombo.getItems().clear());
            return;
        }
        if (keyspace == null || keyspace.isEmpty()) {
            Platform.runLater(() -> tableCombo.getItems().clear());
            return;
        }
        final String ks = keyspace;
        Task<List<String>> task = new Task<>() {
            @Override
            protected List<String> call() {
                return CassandraSchemaService.listTables(session, ks);
            }
        };
        task.setOnSucceeded(ev -> Platform.runLater(() -> {
            schemaListenersMuted = true;
            tableCombo.getItems().setAll(task.getValue());
            schemaListenersMuted = false;
        }));
        task.setOnFailed(ev -> Platform.runLater(() -> tableCombo.getItems().clear()));
        executor.submit(task);
    }

    @FXML
    private void onRefreshSchema() {
        if (session == null) {
            alert(Alert.AlertType.WARNING, "Сначала подключитесь к Cassandra.");
            return;
        }
        refreshKeyspacesFromClusterAsync();
    }

    private static CsvTableData alignCsvRowsToSchema(List<String> schemaColumns, CsvTableData raw) {
        int n = schemaColumns.size();
        List<List<String>> alignedRows = new ArrayList<>();
        for (List<String> row : raw.getRows()) {
            List<String> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(i < row.size() && row.get(i) != null ? row.get(i) : "");
            }
            alignedRows.add(out);
        }
        return new CsvTableData(schemaColumns, alignedRows);
    }

    @FXML
    private void onAddFiles() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Выберите CSV файлы");
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("CSV", "*.csv"));
        chooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Все файлы", "*.*"));
        Window owner = filesListView.getScene().getWindow();
        List<java.io.File> chosen = chooser.showOpenMultipleDialog(owner);
        if (chosen == null) {
            return;
        }
        for (java.io.File f : chosen) {
            String abs = f.getAbsolutePath();
            if (!filePaths.contains(abs)) {
                filePaths.add(abs);
            }
        }
    }

    @FXML
    private void onRemoveFile() {
        String sel = filesListView.getSelectionModel().getSelectedItem();
        if (sel != null) {
            filePaths.remove(sel);
        }
    }

    @FXML
    private void onPreview() {
        if (session == null) {
            alert(Alert.AlertType.WARNING,
                    "Подключитесь к кластеру и выберите keyspace и таблицу — имена колонок подставляются из схемы.");
            return;
        }
        Path path = selectedPath();
        if (path == null) {
            return;
        }
        String ks = comboText(keyspaceCombo);
        String tb = comboText(tableCombo);
        if (ks.isEmpty() || tb.isEmpty()) {
            alert(Alert.AlertType.WARNING, "Выберите keyspace и таблицу из списка (или введите вручную после подключения).");
            return;
        }
        commitSpinner(recordsSpinner);
        try {
            Optional<List<String>> schemaOpt = CassandraSchemaService.getColumnNamesOrdered(session, ks, tb);
            if (schemaOpt.isEmpty()) {
                alert(Alert.AlertType.ERROR, "Таблица не найдена в metadata: " + ks + "." + tb);
                return;
            }
            List<String> schemaCols = schemaOpt.get();
            boolean headerRow = firstRowIsHeaderCheck.isSelected();
            CsvTableData raw = csvParseService.readAll(path, StandardCharsets.UTF_8, headerRow);
            if (!raw.getRows().isEmpty()) {
                int w = raw.getRows().get(0).size();
                if (w > schemaCols.size()) {
                    appendLog("Предупреждение: в CSV больше полей, чем колонок в таблице — лишние поля игнорируются.");
                } else if (w < schemaCols.size()) {
                    appendLog("Предупреждение: в CSV меньше полей, чем колонок в таблице — недостающие заполняются пустой строкой.");
                }
            }
            CsvTableData data = alignCsvRowsToSchema(schemaCols, raw);
            lastParsedRowCount = data.rowCount();
            updateRecordsSpinnerMax(lastParsedRowCount);
            populateDataTable(previewTable, data.getColumnNames(), data.getRows(), false);
            appendLog(String.format(
                    "Предпросмотр: «%s», схема %s.%s (%d колонок), строк данных: %d, первая строка файла пропущена: %s",
                    path.getFileName(),
                    ks,
                    tb,
                    schemaCols.size(),
                    lastParsedRowCount,
                    headerRow ? "да" : "нет"));
        } catch (IOException | CsvException e) {
            alert(Alert.AlertType.ERROR, "Не удалось прочитать CSV:\n" + e.getMessage());
        }
    }

    private void updateRecordsSpinnerMax(int dataRows) {
        int max = Math.max(1, dataRows);
        int current = recordsSpinner.getValue();
        SpinnerValueFactory.IntegerSpinnerValueFactory f =
                new SpinnerValueFactory.IntegerSpinnerValueFactory(1, max, Math.min(current, max));
        recordsSpinner.setValueFactory(f);
    }

    /**
     * @param csvColumnMapping для CSV: заголовок «исходное → "cql"»; для результата SELECT — только имя колонки
     */
    private void populateDataTable(
            TableView<ObservableList<String>> table,
            List<String> columnTitles,
            List<List<String>> dataRows,
            boolean csvColumnMapping) {
        table.getColumns().clear();
        table.getItems().clear();
        for (int i = 0; i < columnTitles.size(); i++) {
            final int colIndex = i;
            String header = columnTitles.get(i);
            String title;
            if (csvColumnMapping) {
                String cqlName = CqlIdentifiers.sanitize(header);
                title = header.isBlank() ? "\"" + cqlName + "\"" : header + " → \"" + cqlName + "\"";
            } else {
                title = header;
            }
            TableColumn<ObservableList<String>, String> col = new TableColumn<>(title);
            col.setCellValueFactory(cell ->
                    new ReadOnlyObjectWrapper<>(
                            colIndex < cell.getValue().size() ? cell.getValue().get(colIndex) : ""));
            col.setPrefWidth(120);
            table.getColumns().add(col);
        }
        ObservableList<ObservableList<String>> items = FXCollections.observableArrayList();
        int show = Math.min(PREVIEW_ROW_CAP, dataRows.size());
        for (int r = 0; r < show; r++) {
            items.add(FXCollections.observableArrayList(dataRows.get(r)));
        }
        table.setItems(items);
    }

    @FXML
    private void onSelectFromDb() {
        if (session == null) {
            alert(Alert.AlertType.WARNING, "Сначала подключитесь к Cassandra.");
            return;
        }
        String ks = comboText(keyspaceCombo);
        String tb = comboText(tableCombo);
        if (ks.isEmpty() || tb.isEmpty()) {
            alert(Alert.AlertType.WARNING, "Укажите keyspace и имя таблицы.");
            return;
        }
        selectFromDbButton.setDisable(true);
        Task<CassandraSelectService.TablePreview> task = new Task<>() {
            @Override
            protected CassandraSelectService.TablePreview call() {
                return selectService.selectStarLimit(session, ks, tb, 5);
            }
        };
        task.setOnSucceeded(ev -> {
            selectFromDbButton.setDisable(false);
            CassandraSelectService.TablePreview preview = task.getValue();
            populateDataTable(dbDataTable, preview.getColumnNames(), preview.getRows(), false);
            appendLog(String.format(
                    "SELECT * FROM \"%s\".\"%s\" LIMIT 5 — колонок: %d, строк: %d",
                    ks,
                    tb,
                    preview.getColumnNames().size(),
                    preview.getRows().size()));
        });
        task.setOnFailed(ev -> {
            selectFromDbButton.setDisable(false);
            Throwable ex = task.getException();
            String msg = ex != null ? ex.getMessage() : "ошибка";
            appendLog("Ошибка SELECT: " + msg);
            alert(Alert.AlertType.ERROR, msg);
        });
        executor.submit(task);
    }

    @FXML
    private void onStartLoad() {
        if (session == null) {
            alert(Alert.AlertType.WARNING, "Сначала подключитесь к Cassandra.");
            return;
        }
        Path path = selectedPath();
        if (path == null) {
            return;
        }
        String ks = comboText(keyspaceCombo);
        String tb = comboText(tableCombo);
        if (ks.isEmpty() || tb.isEmpty()) {
            alert(Alert.AlertType.WARNING, "Укажите keyspace и имя таблицы.");
            return;
        }
        commitSpinner(recordsSpinner);
        int maxRows = recordsSpinner.getValue();
        loadButton.setDisable(true);
        previewButton.setDisable(true);
        connectButton.setDisable(true);
        loadProgressBar.setProgress(ProgressBar.INDETERMINATE_PROGRESS);

        Task<CassandraBulkLoadService.LoadResult> task = new Task<>() {
            @Override
            protected CassandraBulkLoadService.LoadResult call() throws Exception {
                List<String> schemaCols = CassandraSchemaService.getColumnNamesOrdered(session, ks, tb)
                        .orElseThrow(() -> new IllegalArgumentException("Таблица не найдена в metadata: " + ks + "." + tb));
                boolean headerRow = firstRowIsHeaderCheck.isSelected();
                CsvTableData raw = csvParseService.readAll(path, StandardCharsets.UTF_8, headerRow);
                CsvTableData data = alignCsvRowsToSchema(schemaCols, raw);
                int totalRows = data.rowCount();
                int planned = Math.min(maxRows, totalRows);
                Platform.runLater(() -> {
                    loadProgressBar.setProgress(0);
                    progressDetailLabel.setText("Загрузка…");
                    countersLabel.setText(String.format(
                            "Загружено: 0 | Осталось: %d | Всего в плане: %d", planned, planned));
                });
                appendLog("— Начало загрузки —");
                appendLog(String.format(
                        "Файл: %s, keyspace.table: %s.%s, колонок по схеме: %d, записей к загрузке (макс.): %d, строк данных в CSV: %d",
                        path, ks, tb, schemaCols.size(), maxRows, totalRows));
                CassandraBulkLoadService.ProgressListener progressListener =
                        (successful, processed, total) -> Platform.runLater(() -> {
                            loadProgressBar.setProgress(total == 0 ? 1 : (double) processed / total);
                            int left = total - processed;
                            countersLabel.setText(String.format(
                                    "Загружено успешно: %d | Обработано: %d / %d | Осталось: %d",
                                    successful, processed, total, left));
                            progressDetailLabel.setText(String.format("Обработка строки %d из %d", processed, total));
                        });
                return bulkLoadService.insertRows(
                        session,
                        ks,
                        tb,
                        data,
                        schemaCols,
                        maxRows,
                        MainController.this::appendLog,
                        progressListener);
            }
        };
        task.setOnSucceeded(ev -> {
            loadButton.setDisable(false);
            previewButton.setDisable(false);
            connectButton.setDisable(false);
            loadProgressBar.setProgress(1);
            CassandraBulkLoadService.LoadResult r = task.getValue();
            progressDetailLabel.setText(String.format("Завершено: успех %d, ошибок %d", r.inserted(), r.failed()));
            appendLog("— Загрузка завершена —");
        });
        task.setOnFailed(ev -> {
            loadButton.setDisable(false);
            previewButton.setDisable(false);
            connectButton.setDisable(false);
            loadProgressBar.setProgress(0);
            Throwable ex = task.getException();
            String msg = ex != null ? ex.getMessage() : "ошибка";
            appendLog("Критическая ошибка: " + msg);
            progressDetailLabel.setText("Ошибка");
            alert(Alert.AlertType.ERROR, msg);
        });
        executor.submit(task);
    }

    private Path selectedPath() {
        String sel = filesListView.getSelectionModel().getSelectedItem();
        if (sel == null) {
            if (filePaths.isEmpty()) {
                alert(Alert.AlertType.WARNING, "Добавьте CSV файл и выберите его в списке.");
            } else {
                alert(Alert.AlertType.WARNING, "Выберите файл в списке.");
            }
            return null;
        }
        return Path.of(sel);
    }

    private void appendLog(String message) {
        String line = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")) + " " + message + "\n";
        if (Platform.isFxApplicationThread()) {
            logArea.appendText(line);
        } else {
            Platform.runLater(() -> logArea.appendText(line));
        }
    }

    private void alert(Alert.AlertType type, String text) {
        Platform.runLater(() -> {
            Alert a = new Alert(type, text);
            a.setHeaderText(null);
            a.showAndWait();
        });
    }

    private void closeSessionQuietly() {
        if (session != null) {
            try {
                session.close();
            } catch (Exception ignored) {
                // ignore
            }
            session = null;
        }
    }

    private void shutdown() {
        closeSessionQuietly();
        executor.shutdownNow();
    }
}
