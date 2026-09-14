package io.quarkus.agent.mcp;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The streaming path reimplements three things that already existed for the in-memory path:
 * finding a fragment's source, collecting every source it writes, and splitting SQL into
 * statements. All three have to agree with the originals: which path a fragment takes is
 * decided by where it came from, not by anything the rest of the loader can see.
 */
class RagSqlStreamingTest {

    /** The first lines of the real aggregated core artifact: comments, then DELETE, then rows. */
    private static final String AGGREGATED_HEAD = """
            -- quarkus-rag fragment: quarkus-documentation 3.38.1
            -- Generated: 2026-08-04T11:04:45.224724006Z

            DELETE FROM rag_documents WHERE metadata->>'source' = 'quarkus-documentation';

            INSERT INTO rag_documents (embedding_id, embedding, text, metadata) VALUES ('e46b60f5', \
            '[0.1,0.2]'::vector, 'text', '{"source":"quarkus-README","version":"3.38.1"}'::jsonb);
            INSERT INTO rag_documents (embedding_id, embedding, text, metadata) VALUES ('a1b2c3d4', \
            '[0.3,0.4]'::vector, 'more', '{"source":"quarkus-rest","version":"3.38.1"}'::jsonb);
            """;

    private static String peek(String sql, String fallback) throws IOException {
        return RagSqlLoader.peekSource(new BufferedReader(new StringReader(sql)), fallback);
    }

    private static List<String> streamSplit(String sql) throws IOException, SQLException {
        List<String> statements = new ArrayList<>();
        RagSqlLoader.streamSplitAndConsume(
                new PushbackReader(new StringReader(sql), 1), statements::add);
        return statements;
    }

    // ── peekSource agrees with extractSource ────────────────────────────────────

    @Test
    void peekPrefersARowSourceOverTheDeleteName() throws IOException {
        // The aggregated artifact deletes by 'quarkus-documentation' but writes per-guide
        // sources. Identifying it by the DELETE name would match no row in rag_documents, so
        // it would look unloaded on every restart and re-run into its baked-in primary keys.
        assertEquals("quarkus-README", peek(AGGREGATED_HEAD, "quarkus-documentation"));
        assertEquals(RagSqlLoader.extractSource(AGGREGATED_HEAD, "quarkus-documentation"),
                peek(AGGREGATED_HEAD, "quarkus-documentation"));
    }

    @Test
    void peekFallsBackToTheDeleteNameWhenThereAreNoRows() throws IOException {
        String sql = "DELETE FROM rag_documents WHERE metadata->>'source' = 'quarkus-rest';\n";

        assertEquals("quarkus-rest", peek(sql, "fallback"));
        assertEquals(RagSqlLoader.extractSource(sql, "fallback"), peek(sql, "fallback"));
    }

    @Test
    void peekUsesTheFallbackWhenNoSourceIsNamedAtAll() throws IOException {
        String sql = "INSERT INTO rag_documents VALUES (1);\n";

        assertEquals("my-extension", peek(sql, "my-extension"));
        assertEquals(RagSqlLoader.extractSource(sql, "my-extension"), peek(sql, "my-extension"));
    }

    @Test
    void peekHandlesWhitespaceVariationsLikeExtractSourceDoes() throws IOException {
        String sql = "INSERT INTO rag_documents VALUES ('{\"source\"  :  \"quarkus-hibernate-orm\"}');\n";

        assertEquals("quarkus-hibernate-orm", peek(sql, "fallback"));
        assertEquals(RagSqlLoader.extractSource(sql, "fallback"), peek(sql, "fallback"));
    }

    // ── streamSplitAndConsume agrees with splitSqlStatements ────────────────────

    @Test
    void streamSplitHandlesSemicolonsInQuotedStrings() throws Exception {
        String sql = "DELETE FROM t WHERE x = 'a;b';\nINSERT INTO t VALUES ('c;d');";

        assertEquals(RagSqlLoader.splitSqlStatements(sql), streamSplit(sql));
    }

    @Test
    void streamSplitHandlesEscapedQuotes() throws Exception {
        String sql = "INSERT INTO t VALUES ('it''s a test; with semicolons');\n"
                + "INSERT INTO t VALUES ('import java.util.UUID;');";

        assertEquals(RagSqlLoader.splitSqlStatements(sql), streamSplit(sql));
    }

    @Test
    void streamSplitHandlesMultipleEscapedQuotes() throws Exception {
        String sql = "INSERT INTO t VALUES ('don''t stop; can''t stop');\nDELETE FROM t WHERE x = 'y';";

        assertEquals(RagSqlLoader.splitSqlStatements(sql), streamSplit(sql));
    }

    @Test
    void streamSplitSkipsComments() throws Exception {
        String sql = "-- this is a comment\nINSERT INTO t VALUES (1);";

        assertEquals(RagSqlLoader.splitSqlStatements(sql), streamSplit(sql));
    }

    @Test
    void streamSplitKeepsHyphensThatAreNotComments() throws Exception {
        // A single '-' needs the lookahead char pushed back, not swallowed
        String sql = "INSERT INTO t VALUES (5-3, 'a-b');\nINSERT INTO t VALUES ('x');";

        assertEquals(RagSqlLoader.splitSqlStatements(sql), streamSplit(sql));
    }

    @Test
    void streamSplitKeepsDoubleHyphensInsideQuotedText() throws Exception {
        // '--' inside a string literal is data, not a comment
        String sql = "INSERT INTO t VALUES ('see -- this is kept; really');\nDELETE FROM t;";

        assertEquals(RagSqlLoader.splitSqlStatements(sql), streamSplit(sql));
    }

    @Test
    void streamSplitEmitsATrailingStatementWithoutASemicolon() throws Exception {
        String sql = "INSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2)";

        assertEquals(RagSqlLoader.splitSqlStatements(sql), streamSplit(sql));
    }

    @Test
    void streamSplitMatchesOnTheAggregatedArtifactShape() throws Exception {
        assertEquals(RagSqlLoader.splitSqlStatements(AGGREGATED_HEAD), streamSplit(AGGREGATED_HEAD));
    }

    // ── streamSources agrees with extractSources ────────────────────────────────

    private static Set<String> streamSources(String sql, String fallback) throws IOException {
        return RagSqlLoader.streamSources(new BufferedReader(new StringReader(sql)), fallback);
    }

    @Test
    void streamSourcesFindsEverySourceTheFragmentWrites() throws IOException {
        // Deciding what to clear before a reload needs all of them, not just the peeked one
        assertEquals(RagSqlLoader.extractSources(AGGREGATED_HEAD, "fallback"),
                streamSources(AGGREGATED_HEAD, "fallback"));
    }

    @Test
    void streamSourcesFallsBackWhenTheFragmentNamesNoSource() throws IOException {
        String sql = "INSERT INTO rag_documents VALUES (1);\n";

        assertEquals(RagSqlLoader.extractSources(sql, "quarkus-hibernate-orm"),
                streamSources(sql, "quarkus-hibernate-orm"));
    }

    @Test
    void streamSourcesKeepsTheDeleteNameAlongsideTheRowSources() throws IOException {
        // Unlike peekSource, which picks one, a reload has to clear every name the
        // fragment touches, including the one its own DELETE was generated under
        assertTrue(streamSources(AGGREGATED_HEAD, "fallback").contains("quarkus-documentation"));
    }
}
