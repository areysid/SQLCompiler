import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import javax.swing.*;
import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

class Token {
    String type;
    String value;
    int line, column;

    Token(String type, String value, int line, int column) {
        this.type = type;
        this.value = value;
        this.line = line;
        this.column = column;
    }
}

class Database {
    private Map<String, Map<String, String>> tableSchemas = new HashMap<>();
    private Map<String, java.util.List<Map<String, Object>>> tableData = new HashMap<>();

    void createTable(String tableName, Map<String, String> columns) {
        if (tableSchemas.containsKey(tableName)) {
            System.out.println("Table '" + tableName + "' already exists, skipping creation.");
            return;
        }
        tableSchemas.put(tableName, columns);
        tableData.put(tableName, new ArrayList<>());
        System.out.println("Created table '" + tableName + "'.");
    }

    void alterTable(String tableName, String columnName, String columnType) {
        if (!tableSchemas.containsKey(tableName)) throw new RuntimeException("Table '" + tableName + "' not found.");
        tableSchemas.get(tableName).put(columnName, columnType);
        System.out.println("Altered table '" + tableName + "' to add '" + columnName + "'.");
    }

    void dropTable(String tableName) {
        if (!tableSchemas.containsKey(tableName)) throw new RuntimeException("Table '" + tableName + "' not found.");
        tableSchemas.remove(tableName);
        tableData.remove(tableName);
        System.out.println("Dropped table '" + tableName + "'.");
    }

    void insert(String tableName, java.util.List<Object> values) {
        if (!tableSchemas.containsKey(tableName)) throw new RuntimeException("Table '" + tableName + "' not found.");
        Map<String, String> schema = tableSchemas.get(tableName);
        if (values.size() != schema.size()) throw new RuntimeException("Expected " + schema.size() + " values, got " + values.size());
        Map<String, Object> row = new HashMap<>();
        int i = 0;
        for (String col : schema.keySet()) row.put(col, values.get(i++));
        tableData.get(tableName).add(row);
        System.out.println("Inserted into '" + tableName + "'.");
    }

    java.util.List<Map<String, Object>> select(String tableName, String joinTable, String joinColumn1, String joinColumn2, String whereColumn, Object whereValue, String likePattern, boolean isNullCheck, boolean isNotNullCheck, String groupByColumn, String orderByColumn) {
        if (!tableSchemas.containsKey(tableName)) throw new RuntimeException("Table '" + tableName + "' not found.");
        java.util.List<Map<String, Object>> result = new ArrayList<>();
        java.util.List<Map<String, Object>> baseRows = tableData.get(tableName);

        if (joinTable != null) {
            if (!tableSchemas.containsKey(joinTable)) throw new RuntimeException("Join table '" + joinTable + "' not found.");
            java.util.List<Map<String, Object>> joinRows = tableData.get(joinTable);
            for (Map<String, Object> baseRow : baseRows) {
                for (Map<String, Object> joinRow : joinRows) {
                    if (baseRow.get(joinColumn1).equals(joinRow.get(joinColumn2))) {
                        Map<String, Object> combined = new HashMap<>(baseRow);
                        joinRow.forEach((k, v) -> combined.put(joinTable + "." + k, v));
                        result.add(combined);
                    }
                }
            }
        } else {
            result.addAll(baseRows);
        }

        if (whereColumn != null || likePattern != null || isNullCheck || isNotNullCheck) {
            result.removeIf(row -> {
                if (whereColumn != null && !row.get(whereColumn).equals(whereValue)) return true;
                if (likePattern != null && (row.get(whereColumn) == null || !((String) row.get(whereColumn)).matches(likePattern.replace("%", ".*")))) return true;
                if (isNullCheck && row.get(whereColumn) != null) return true;
                if (isNotNullCheck && row.get(whereColumn) == null) return true;
                return false;
            });
        }

        if (groupByColumn != null) {
            Map<Object, java.util.List<Map<String, Object>>> grouped = new LinkedHashMap<>();
            for (Map<String, Object> row : result) {
                Object key = row.get(groupByColumn);
                grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
            result.clear();
            grouped.forEach((key, rows) -> {
                Map<String, Object> aggRow = new HashMap<>();
                aggRow.put(groupByColumn, key);
                aggRow.put("COUNT", (double) rows.size());
                result.add(aggRow);
            });
        }

        if (orderByColumn != null) {
            result.sort((a, b) -> {
                Object valA = a.get(orderByColumn);
                Object valB = b.get(orderByColumn);
                if (valA instanceof Double && valB instanceof Double) return Double.compare((Double) valA, (Double) valB);
                return valA.toString().compareTo(valB.toString());
            });
        }

        return result;
    }

    void delete(String tableName, String whereColumn, Object whereValue, String likePattern, boolean isNullCheck, boolean isNotNullCheck) {
        if (!tableSchemas.containsKey(tableName)) throw new RuntimeException("Table '" + tableName + "' not found.");
        java.util.List<Map<String, Object>> rows = tableData.get(tableName);
        int removed = 0;
        Iterator<Map<String, Object>> iterator = rows.iterator();
        while (iterator.hasNext()) {
            Map<String, Object> row = iterator.next();
            boolean match = true;
            if (whereColumn != null && !row.get(whereColumn).equals(whereValue)) match = false;
            if (likePattern != null && (row.get(whereColumn) == null || !((String) row.get(whereColumn)).matches(likePattern.replace("%", ".*")))) match = false;
            if (isNullCheck && row.get(whereColumn) != null) match = false;
            if (isNotNullCheck && row.get(whereColumn) == null) match = false;
            if (match) {
                iterator.remove();
                removed++;
            }
        }
        System.out.println("Deleted " + removed + " row(s) from '" + tableName + "'.");
    }

    void update(String tableName, String setColumn, Object setValue, String whereColumn, Object whereValue, String likePattern, boolean isNullCheck, boolean isNotNullCheck) {
        if (!tableSchemas.containsKey(tableName)) throw new RuntimeException("Table '" + tableName + "' not found.");
        if (!tableSchemas.get(tableName).containsKey(setColumn)) throw new RuntimeException("Column '" + setColumn + "' not found.");
        java.util.List<Map<String, Object>> rows = tableData.get(tableName);
        int updated = 0;
        for (Map<String, Object> row : rows) {
            boolean match = true;
            if (whereColumn != null && !row.get(whereColumn).equals(whereValue)) match = false;
            if (likePattern != null && (row.get(whereColumn) == null || !((String) row.get(whereColumn)).matches(likePattern.replace("%", ".*")))) match = false;
            if (isNullCheck && row.get(whereColumn) != null) match = false;
            if (isNotNullCheck && row.get(whereColumn) == null) match = false;
            if (match) {
                row.put(setColumn, setValue);
                updated++;
            }
        }
        System.out.println("Updated " + updated + " row(s) in '" + tableName + "'.");
    }

    void saveToFile(String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (String tableName : tableSchemas.keySet()) {
                writer.write("TABLE " + tableName + "\n");
                Map<String, String> schema = tableSchemas.get(tableName);
                writer.write("SCHEMA ");
                for (Map.Entry<String, String> col : schema.entrySet()) writer.write(col.getKey() + ":" + col.getValue() + ",");
                writer.newLine();
                java.util.List<Map<String, Object>> rows = tableData.get(tableName);
                for (Map<String, Object> row : rows) {
                    writer.write("ROW ");
                    for (Map.Entry<String, Object> entry : row.entrySet()) writer.write(entry.getKey() + "=" + entry.getValue() + ",");
                    writer.newLine();
                }
                writer.write("END\n");
            }
        }
        System.out.println("Database saved to " + filename);
    }

    void loadFromFile(String filename) throws IOException {
        tableSchemas.clear();
        tableData.clear();
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            String currentTable = null;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("TABLE ")) {
                    currentTable = line.substring(6);
                    tableSchemas.put(currentTable, new HashMap<>());
                    tableData.put(currentTable, new ArrayList<>());
                } else if (line.startsWith("SCHEMA ") && currentTable != null) {
                    String[] parts = line.substring(7).split(",");
                    for (String part : parts) {
                        if (!part.isEmpty()) {
                            String[] col = part.split(":");
                            tableSchemas.get(currentTable).put(col[0], col[1]);
                        }
                    }
                } else if (line.startsWith("ROW ") && currentTable != null) {
                    Map<String, Object> row = new HashMap<>();
                    String[] parts = line.substring(4).split(",");
                    for (String part : parts) {
                        if (!part.isEmpty()) {
                            String[] kv = part.split("=");
                            String value = kv[1];
                            if (value.matches("-?\\d+\\.?\\d*")) row.put(kv[0], Double.parseDouble(value));
                            else row.put(kv[0], value);
                        }
                    }
                    tableData.get(currentTable).add(row);
                }
            }
        }
        System.out.println("Database loaded from " + filename);
    }
}

class ASTNode {
    String type;
    String tableName;
    Map<String, String> columns;
    java.util.List<Object> values;
    String setColumn;
    Object setValue;
    String whereColumn;
    Object whereValue;
    String likePattern;
    boolean isNullCheck;
    boolean isNotNullCheck;
    String joinTable;
    String joinColumn1;
    String joinColumn2;
    String groupByColumn;
    String orderByColumn;

    ASTNode(String type) {
        this.type = type;
    }
}

class Lexer {
    private String input;
    private java.util.List<Token> tokens = new ArrayList<>();
    private static final String[] KEYWORDS = {"CREATE", "TABLE", "INSERT", "INTO", "VALUES", "SELECT", "FROM", "DELETE", "UPDATE", "SET", "WHERE", "ALTER", "ADD", "DROP", "GROUP", "BY", "ORDER", "JOIN", "ON", "LIKE", "IS", "NULL", "NOT"};
    private static final String OPERATORS = "(),*=;";
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?[0-9]+(\\.[0-9]+)?");
    private static final Pattern STRING_PATTERN = Pattern.compile("'[^']*'");

    Lexer(String input) {
        this.input = input;
    }

    java.util.List<Token> tokenize() {
        int line = 1, column = 1;
        String[] lines = input.split("\n");
        for (String currentLine : lines) {
            int i = 0;
            column = 1;
            while (i < currentLine.length()) {
                char ch = currentLine.charAt(i);
                if (Character.isWhitespace(ch)) {
                    i++;
                    column++;
                    continue;
                }
                if (OPERATORS.indexOf(ch) != -1) {
                    tokens.add(new Token("OPERATOR", String.valueOf(ch), line, column));
                    i++;
                    column++;
                    continue;
                }
                String rest = currentLine.substring(i);
                Matcher idMatcher = IDENTIFIER_PATTERN.matcher(rest);
                if (idMatcher.lookingAt()) {
                    String word = idMatcher.group();
                    String type = Arrays.asList(KEYWORDS).contains(word.toUpperCase()) ? "KEYWORD" : "IDENTIFIER";
                    tokens.add(new Token(type, word.toUpperCase(), line, column));
                    i += word.length();
                    column += word.length();
                    continue;
                }
                Matcher numMatcher = NUMBER_PATTERN.matcher(rest);
                if (numMatcher.lookingAt()) {
                    String number = numMatcher.group();
                    tokens.add(new Token("NUMBER", number, line, column));
                    i += number.length();
                    column += number.length();
                    continue;
                }
                Matcher strMatcher = STRING_PATTERN.matcher(rest);
                if (strMatcher.lookingAt()) {
                    String str = strMatcher.group();
                    tokens.add(new Token("STRING", str.substring(1, str.length() - 1), line, column));
                    i += str.length();
                    column += str.length();
                    continue;
                }
                i++;
                column++;
            }
            line++;
        }
        return tokens;
    }
}

class Parser {
    private java.util.List<Token> tokens;
    private int pos = 0;

    Parser(java.util.List<Token> tokens) {
        this.tokens = tokens;
    }

    java.util.List<ASTNode> parse() {
        java.util.List<ASTNode> statements = new ArrayList<>();
        while (pos < tokens.size()) {
            ASTNode stmt = parseStatement();
            if (stmt != null) statements.add(stmt);
            if (pos < tokens.size() && ";".equals(currentToken().value)) pos++;
        }
        return statements;
    }

    private ASTNode parseStatement() {
        Token current = currentToken();
        if (current == null) return null;
        if ("KEYWORD".equals(current.type)) {
            switch (current.value) {
                case "CREATE": return parseCreateTable();
                case "INSERT": return parseInsert();
                case "SELECT": return parseSelect();
                case "DELETE": return parseDelete();
                case "UPDATE": return parseUpdate();
                case "ALTER": return parseAlterTable();
                case "DROP": return parseDropTable();
            }
        }
        pos++;
        return null;
    }

    private ASTNode parseCreateTable() {
        consume("KEYWORD", "CREATE");
        consume("KEYWORD", "TABLE");
        Token tableName = consume("IDENTIFIER");
        consume("OPERATOR", "(");
        Map<String, String> columns = new HashMap<>();
        while (currentToken() != null && !")".equals(currentToken().value)) {
            Token colName = consume("IDENTIFIER");
            Token colType = consume("IDENTIFIER");
            columns.put(colName.value, colType.value);
            if (currentToken() != null && ",".equals(currentToken().value)) consume("OPERATOR", ",");
        }
        consume("OPERATOR", ")");
        ASTNode node = new ASTNode("CreateTable");
        node.tableName = tableName.value;
        node.columns = columns;
        return node;
    }

    private ASTNode parseInsert() {
        consume("KEYWORD", "INSERT");
        consume("KEYWORD", "INTO");
        Token tableName = consume("IDENTIFIER");
        consume("KEYWORD", "VALUES");
        consume("OPERATOR", "(");
        java.util.List<Object> values = new ArrayList<>();
        while (currentToken() != null && !")".equals(currentToken().value)) {
            Token val = currentToken();
            if ("NUMBER".equals(val.type)) values.add(Double.parseDouble(consume("NUMBER").value));
            else if ("STRING".equals(val.type)) values.add(consume("STRING").value);
            else throw new RuntimeException("Invalid value at " + val.line + ":" + val.column);
            if (currentToken() != null && ",".equals(currentToken().value)) consume("OPERATOR", ",");
        }
        consume("OPERATOR", ")");
        ASTNode node = new ASTNode("Insert");
        node.tableName = tableName.value;
        node.values = values;
        return node;
    }

    private ASTNode parseSelect() {
        consume("KEYWORD", "SELECT");
        consume("OPERATOR", "*");
        consume("KEYWORD", "FROM");
        Token tableName = consume("IDENTIFIER");
        ASTNode node = new ASTNode("Select");
        node.tableName = tableName.value;

        if (currentToken() != null && "JOIN".equals(currentToken().value)) {
            consume("KEYWORD", "JOIN");
            node.joinTable = consume("IDENTIFIER").value;
            consume("KEYWORD", "ON");
            node.joinColumn1 = consume("IDENTIFIER").value;
            consume("OPERATOR", "=");
            node.joinColumn2 = consume("IDENTIFIER").value;
        }

        if (currentToken() != null && "WHERE".equals(currentToken().value)) {
            parseWhereClause(node);
        }

        if (currentToken() != null && "GROUP".equals(currentToken().value)) {
            consume("KEYWORD", "GROUP");
            consume("KEYWORD", "BY");
            node.groupByColumn = consume("IDENTIFIER").value;
        }

        if (currentToken() != null && "ORDER".equals(currentToken().value)) {
            consume("KEYWORD", "ORDER");
            consume("KEYWORD", "BY");
            node.orderByColumn = consume("IDENTIFIER").value;
        }

        return node;
    }

    private ASTNode parseDelete() {
        consume("KEYWORD", "DELETE");
        consume("KEYWORD", "FROM");
        Token tableName = consume("IDENTIFIER");
        ASTNode node = new ASTNode("Delete");
        node.tableName = tableName.value;
        if (currentToken() != null && "WHERE".equals(currentToken().value)) {
            parseWhereClause(node);
        }
        return node;
    }

    private ASTNode parseUpdate() {
        consume("KEYWORD", "UPDATE");
        Token tableName = consume("IDENTIFIER");
        consume("KEYWORD", "SET");
        Token setColumn = consume("IDENTIFIER");
        consume("OPERATOR", "=");
        Token setValueToken = currentToken();
        Object setValue;
        if ("NUMBER".equals(setValueToken.type)) setValue = Double.parseDouble(consume("NUMBER").value);
        else if ("STRING".equals(setValueToken.type)) setValue = consume("STRING").value;
        else throw new RuntimeException("Invalid value at " + setValueToken.line + ":" + setValueToken.column);
        ASTNode node = new ASTNode("Update");
        node.tableName = tableName.value;
        node.setColumn = setColumn.value;
        node.setValue = setValue;
        if (currentToken() != null && "WHERE".equals(currentToken().value)) {
            parseWhereClause(node);
        }
        return node;
    }

    private ASTNode parseAlterTable() {
        consume("KEYWORD", "ALTER");
        consume("KEYWORD", "TABLE");
        Token tableName = consume("IDENTIFIER");
        consume("KEYWORD", "ADD");
        Token columnName = consume("IDENTIFIER");
        Token columnType = consume("IDENTIFIER");
        ASTNode node = new ASTNode("AlterTable");
        node.tableName = tableName.value;
        node.columns = new HashMap<>();
        node.columns.put(columnName.value, columnType.value);
        return node;
    }

    private ASTNode parseDropTable() {
        consume("KEYWORD", "DROP");
        consume("KEYWORD", "TABLE");
        Token tableName = consume("IDENTIFIER");
        ASTNode node = new ASTNode("DropTable");
        node.tableName = tableName.value;
        return node;
    }

    private void parseWhereClause(ASTNode node) {
        consume("KEYWORD", "WHERE");
        Token column = consume("IDENTIFIER");
        node.whereColumn = column.value;
        if ("LIKE".equals(currentToken().value)) {
            consume("KEYWORD", "LIKE");
            node.likePattern = consume("STRING").value;
        } else if ("IS".equals(currentToken().value)) {
            consume("KEYWORD", "IS");
            if ("NULL".equals(currentToken().value)) {
                consume("KEYWORD", "NULL");
                node.isNullCheck = true;
            } else if ("NOT".equals(currentToken().value)) {
                consume("KEYWORD", "NOT");
                consume("KEYWORD", "NULL");
                node.isNotNullCheck = true;
            }
        } else {
            consume("OPERATOR", "=");
            Token valueToken = currentToken();
            if ("NUMBER".equals(valueToken.type)) node.whereValue = Double.parseDouble(consume("NUMBER").value);
            else if ("STRING".equals(valueToken.type)) node.whereValue = consume("STRING").value;
            else throw new RuntimeException("Invalid value at " + valueToken.line + ":" + valueToken.column);
        }
    }

    private Token currentToken() {
        return pos < tokens.size() ? tokens.get(pos) : null;
    }

    private Token consume(String expectedType) {
        Token current = currentToken();
        if (current != null && expectedType.equals(current.type)) {
            pos++;
            return current;
        }
        throw new RuntimeException("Expected " + expectedType + " at " + (current != null ? current.line + ":" + current.column : "EOF"));
    }

    private Token consume(String expectedType, String expectedValue) {
        Token current = currentToken();
        if (current != null && expectedType.equals(current.type) && expectedValue.equals(current.value)) {
            pos++;
            return current;
        }
        throw new RuntimeException("Expected '" + expectedValue + "' at " + (current != null ? current.line + ":" + current.column : "EOF"));
    }
}

public class SQLCompilerGUI {
    private Database db = new Database();
    private JTextArea inputArea;
    private JTextArea outputArea;
    private String input;

    public SQLCompilerGUI() {
        createAndShowGUI();
    }

    private void createAndShowGUI() {
        JFrame frame = new JFrame("SQL Compiler");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(600, 400);
        frame.setLayout(new BorderLayout());

        inputArea = new JTextArea(10, 50);
        inputArea.setText("Enter your SQL commands here...\n");
        JScrollPane inputScroll = new JScrollPane(inputArea);
        frame.add(inputScroll, BorderLayout.NORTH);

        outputArea = new JTextArea(10, 50);
        outputArea.setEditable(false);
        JScrollPane outputScroll = new JScrollPane(outputArea);
        frame.add(outputScroll, BorderLayout.CENTER);

        JPanel buttonPanel = new JPanel();
        JButton runButton = new JButton("Run SQL");
        buttonPanel.add(runButton);
        frame.add(buttonPanel, BorderLayout.SOUTH);

        PrintStream printStream = new PrintStream(new CustomOutputStream(outputArea));
        System.setOut(printStream);
        System.setErr(printStream);

        runButton.addActionListener(e -> run());

        frame.setVisible(true);
    }

    private static String preprocessInput(String rawInput) {
        StringBuilder result = new StringBuilder();
        String[] lines = rawInput.split("\n");
        for (String line : lines) {
            line = line.replaceAll("--.*$", "").replaceAll("/\\*.*?\\*/", "").trim();
            if (!line.isEmpty()) result.append(line.replaceAll("\\s+", " ")).append("\n");
        }
        return result.toString();
    }

    public void run() {
        try {
            db.loadFromFile("database.txt");
        } catch (IOException e) {
            System.out.println("Starting fresh (no existing database found).");
        }
        String rawInput = inputArea.getText();
        this.input = preprocessInput(rawInput);
        java.util.List<Token> tokens = new Lexer(input).tokenize();
        java.util.List<ASTNode> statements = new Parser(tokens).parse();
        for (ASTNode stmt : statements) {
            execute(stmt);
        }
        try {
            db.saveToFile("database.txt");
        } catch (IOException e) {
            System.err.println("Error saving database: " + e.getMessage());
        }
    }

    private void execute(ASTNode node) {
        if (node == null) return;
        switch (node.type) {
            case "CreateTable":
                db.createTable(node.tableName, node.columns);
                break;
            case "Insert":
                db.insert(node.tableName, node.values);
                break;
            case "Select":
                java.util.List<Map<String, Object>> rows = db.select(node.tableName, node.joinTable, node.joinColumn1, node.joinColumn2, node.whereColumn, node.whereValue, node.likePattern, node.isNullCheck, node.isNotNullCheck, node.groupByColumn, node.orderByColumn);
                System.out.println("Results from '" + node.tableName + "':");
                for (Map<String, Object> row : rows) System.out.println(row);
                break;
            case "Delete":
                db.delete(node.tableName, node.whereColumn, node.whereValue, node.likePattern, node.isNullCheck, node.isNotNullCheck);
                break;
            case "Update":
                db.update(node.tableName, node.setColumn, node.setValue, node.whereColumn, node.whereValue, node.likePattern, node.isNullCheck, node.isNotNullCheck);
                break;
            case "AlterTable":
                db.alterTable(node.tableName, node.columns.keySet().iterator().next(), node.columns.values().iterator().next());
                break;
            case "DropTable":
                db.dropTable(node.tableName);
                break;
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(SQLCompilerGUI::new);
    }
}

class CustomOutputStream extends OutputStream {
    private JTextArea textArea;

    public CustomOutputStream(JTextArea textArea) {
        this.textArea = textArea;
    }

    @Override
    public void write(int b) {
        textArea.append(String.valueOf((char)b));
        textArea.setCaretPosition(textArea.getDocument().getLength());
    }
}