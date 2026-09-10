package io.quarkus.agent.mcp;

import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jboss.logging.Logger;

/**
 * Discovers {@code META-INF/quarkus-rag.sql} fragments from extension deployment JARs
 * in the local Maven repository and loads them into a pgvector database.
 * <p>
 * Supports incremental loading: when new extensions are added to a project,
 * only the new extension's SQL is loaded without reloading existing data.
 * Non-core extensions (Quarkiverse, third-party) are discovered by parsing
 * the project's {@code pom.xml}, following the same pattern as {@link SkillReader}.
 */
@ApplicationScoped
public class RagSqlLoader {

    private static final Logger LOG = Logger.getLogger(RagSqlLoader.class);

    @Inject
    WebClient webClient;

    private static final String RAG_SQL_PATH = "META-INF/quarkus-rag.sql";
    private static final String RAG_DATA_SQL_PATH = "META-INF/quarkus-rag-data.sql";
    private static final String RAG_ARTIFACT_POINTER_PATH = "META-INF/quarkus-rag-artifact.properties";
    private static final String DEPLOYMENT_SUFFIX = "-deployment";
    private static final String CORE_GROUP_ID = "io.quarkus";
    private static final String RAG_DOCUMENTS_TABLE = "rag_documents";

    private static final String CREATE_EXTENSION_DDL = "CREATE EXTENSION IF NOT EXISTS vector";
    private static final String CREATE_TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS rag_documents (
                embedding_id UUID PRIMARY KEY,
                embedding vector(384),
                text TEXT,
                metadata JSONB
            )""";
    private static final String CREATE_INDEX_DDL = """
            CREATE INDEX IF NOT EXISTS idx_rag_embedding ON rag_documents
                USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)""";

    private static final String AGGREGATED_ARTIFACT_ID = "quarkus-documentation-core-rag";
    private static final String AGGREGATED_GROUP_PATH = "io/quarkus";

    private static final Pattern SOURCE_PATTERN = Pattern.compile(
            "metadata\\s*->>\\s*'source'\\s*=\\s*'([^']+)'");
    private static final Pattern ROW_SOURCE_PATTERN = Pattern.compile("\"source\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern EXTENSION_FROM_SOURCE_PATTERN = Pattern.compile(
            "'\\{\"source\":\"([^\"]+)\"");
    private static final String VERSION_KEY = ",\"version\":";
    private static final String QUARKUS_VERSION_KEY = ",\"quarkus_version\":";

    /**
     * @param sql            in-memory SQL content, or {@code null} when {@code streamJarPath} is set
     * @param streamJarPath  when non-null, the fragment's SQL must be streamed line-by-line from the
     *                       {@code META-INF/quarkus-rag(-data).sql} entry of this jar rather than read
     *                       into memory (used for the large aggregated core-docs artifact)
     */
    record RagFragment(String source, String sql, Path streamJarPath) {
        RagFragment(String source, String sql) {
            this(source, sql, null);
        }
    }

    record RagArtifactPointer(String groupId, String artifactId) {
    }

    private final Map<String, Set<String>> loadedSources = new ConcurrentHashMap<>();

    /**
     * Ensures RAG data is loaded for the given Quarkus version.
     * Discovers SQL fragments from core and non-core extension JARs,
     * filters out already-loaded sources, and loads only new data.
     * On first call for a version with a reused container, seeds tracking
     * from the database to avoid redundant loading.
     */
    public void ensureLoaded(String quarkusVersion, String projectDir,
            String host, int port, String database, String user, String password) {
        String versionKey = quarkusVersion != null ? quarkusVersion : "default";
        String jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + database;

        ensureSchema(jdbcUrl, user, password);

        String resolvedVersion = quarkusVersion != null ? quarkusVersion : detectLatestInstalledVersion();
        if (resolvedVersion == null) {
            LOG.warn("Could not determine Quarkus version for RAG loading — no SQL fragments will be loaded");
            return;
        }

        List<RagFragment> allFragments = discoverSqlFragments(resolvedVersion, projectDir);
        if (allFragments.isEmpty()) {
            LOG.infof("No RAG SQL fragments found for Quarkus %s", resolvedVersion);
            return;
        }

        Set<String> alreadyLoaded = loadedSources.computeIfAbsent(versionKey,
                k -> ConcurrentHashMap.newKeySet());

        // On first call for this version, seed from the database (handles container reuse)
        if (alreadyLoaded.isEmpty()) {
            Set<String> existingSources = queryExistingSources(jdbcUrl, user, password);
            alreadyLoaded.addAll(existingSources);
        }

        List<RagFragment> newFragments = allFragments.stream()
                .filter(f -> !alreadyLoaded.contains(f.source()))
                .toList();

        if (newFragments.isEmpty()) {
            LOG.debugf("All %d RAG source(s) already loaded for %s", allFragments.size(), versionKey);
            return;
        }

        loadSql(jdbcUrl, user, password, newFragments, resolvedVersion);

        for (RagFragment f : newFragments) {
            alreadyLoaded.add(f.source());
        }
    }

    /**
     * Discovers RAG SQL fragments from extension deployment JARs in ~/.m2/repository.
     * Checks for the aggregated core artifact first, then scans individual core JARs
     * as a fallback. Always scans non-core extension JARs from the project's pom.xml.
     */
    List<RagFragment> discoverSqlFragments(String quarkusVersion, String projectDir) {
        Path m2Repo = Path.of(System.getProperty("user.home"), ".m2", "repository");

        List<RagFragment> fragments = new ArrayList<>();

        // 1. Core docs: aggregated artifact (preferred) or individual JARs (fallback)
        // The aggregated artifact can be tens of MB (e.g. ~47MB for the current Quarkus
        // documentation artifact, one JSON/vector row per line). We only "peek" the source
        // name here (cheap: reads a handful of lines) and defer the actual content to a
        // streaming fragment that is read/transformed/executed line-by-line in loadSql(),
        // instead of ever materializing the whole file as one in-memory String.
        Path aggregatedJarPath = resolveAggregatedJarPath(quarkusVersion, m2Repo);
        RagFragment aggregated = peekStreamedFragment(aggregatedJarPath, "quarkus-documentation");
        if (aggregated != null) {
            fragments.add(aggregated);
            LOG.infof("Found aggregated RAG SQL artifact locally for Quarkus %s", quarkusVersion);
        } else {
            // Try downloading from Maven Central
            Path downloaded = downloadFromMavenCentral(quarkusVersion, aggregatedJarPath);
            if (downloaded != null) {
                aggregated = peekStreamedFragment(downloaded, "quarkus-documentation");
                if (aggregated != null) {
                    fragments.add(aggregated);
                    LOG.infof("Downloaded aggregated RAG SQL artifact for Quarkus %s", quarkusVersion);
                }
            }
        }

        // Fall back to individual core extension JARs if no aggregated artifact
        if (aggregated == null && Files.isDirectory(m2Repo)) {
            fragments.addAll(scanCoreExtensionJars(m2Repo, quarkusVersion));
        }

        // 2. Non-core extensions: always scan (Quarkiverse, third-party)
        fragments.addAll(scanNonCoreExtensionJars(m2Repo, projectDir, quarkusVersion));

        LOG.infof("Discovered %d RAG SQL fragment(s) for Quarkus %s", fragments.size(), quarkusVersion);
        return fragments;
    }

    private List<RagFragment> scanNonCoreExtensionJars(Path m2Repo, String projectDir, String quarkusVersion) {
        if (projectDir == null) {
            return List.of();
        }

        List<DependencyResolver.Dependency> deps = DependencyResolver.resolve(projectDir);
        if (deps.isEmpty()) {
            return List.of();
        }

        List<RagFragment> fragments = new ArrayList<>();
        for (DependencyResolver.Dependency dep : deps) {
            if (CORE_GROUP_ID.equals(dep.groupId())) {
                continue;
            }
            String groupPath = dep.groupId().replace('.', '/');
            Path deploymentJar = m2Repo.resolve(groupPath)
                    .resolve(dep.artifactId() + DEPLOYMENT_SUFFIX)
                    .resolve(dep.version())
                    .resolve(dep.artifactId() + DEPLOYMENT_SUFFIX + "-" + dep.version() + ".jar");

            if (!Files.isRegularFile(deploymentJar)) {
                continue;
            }

            // Check for a pointer to a separate RAG artifact
            RagArtifactPointer pointer = readRagArtifactPointer(deploymentJar);
            if (pointer != null) {
                RagFragment fragment = resolveExternalRagArtifact(
                        pointer, dep.version(), m2Repo, projectDir);
                if (fragment != null) {
                    String guideUrl = readGuideUrl(m2Repo, dep);
                    fragments.add(injectExtensionMetadata(fragment, dep.artifactId(), quarkusVersion, guideUrl));
                    LOG.debugf("Found RAG SQL via external artifact %s:%s:%s",
                            pointer.groupId(), pointer.artifactId(), dep.version());
                    continue;
                }
            }

            // Fallback: read RAG SQL directly from the deployment JAR
            RagFragment fragment = readFragmentFromJar(deploymentJar, dep.artifactId());
            if (fragment != null) {
                String guideUrl = readGuideUrl(m2Repo, dep);
                fragments.add(injectExtensionMetadata(fragment, dep.artifactId(), quarkusVersion, guideUrl));
                LOG.debugf("Found RAG SQL in non-core extension %s", dep.artifactId());
            }
        }
        return fragments;
    }

    private RagArtifactPointer readRagArtifactPointer(Path jarPath) {
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry entry = jar.getJarEntry(RAG_ARTIFACT_POINTER_PATH);
            if (entry == null) {
                return null;
            }
            Properties props = new Properties();
            try (InputStream is = jar.getInputStream(entry)) {
                props.load(is);
            }
            String groupId = props.getProperty("groupId");
            String artifactId = props.getProperty("artifactId");
            if (groupId == null || artifactId == null) {
                LOG.warnf("Invalid RAG artifact pointer in %s: groupId=%s, artifactId=%s",
                        jarPath, groupId, artifactId);
                return null;
            }
            return new RagArtifactPointer(groupId.trim(), artifactId.trim());
        } catch (IOException e) {
            LOG.debugf("Failed to read RAG artifact pointer from %s: %s",
                    jarPath, e.getMessage());
            return null;
        }
    }

    private RagFragment resolveExternalRagArtifact(
            RagArtifactPointer pointer, String version,
            Path m2Repo, String projectDir) {
        String groupPath = pointer.groupId().replace('.', '/');
        Path ragJarPath = m2Repo.resolve(groupPath)
                .resolve(pointer.artifactId())
                .resolve(version)
                .resolve(pointer.artifactId() + "-" + version + ".jar");

        // Try local first
        RagFragment fragment = readFragmentFromJar(ragJarPath, pointer.artifactId());
        if (fragment != null) {
            return fragment;
        }

        // Try fast HTTP download (works for non-SNAPSHOT releases)
        Path downloaded = downloadArtifact(groupPath, pointer.artifactId(),
                version, ragJarPath, projectDir);
        if (downloaded != null) {
            return readFragmentFromJar(downloaded, pointer.artifactId());
        }

        // Fallback: fetch via Maven (handles SNAPSHOTs and custom repos)
        if (fetchArtifactViaMaven(pointer, version, projectDir)) {
            fragment = readFragmentFromJar(ragJarPath, pointer.artifactId());
            if (fragment != null) {
                return fragment;
            }
        }

        return null;
    }

    private boolean fetchArtifactViaMaven(RagArtifactPointer pointer, String version, String projectDir) {
        if (projectDir == null) {
            return false;
        }
        File dir = new File(projectDir);
        if (!dir.isDirectory()) {
            return false;
        }
        String mvnCmd = ProcessUtils.resolveMavenCommand(dir);
        String artifact = pointer.groupId() + ":" + pointer.artifactId() + ":" + version;
        LOG.infof("RAG artifact not found locally, fetching %s via Maven...", artifact);

        ProcessBuilder pb = new ProcessBuilder(
                mvnCmd, "dependency:get",
                "-Dartifact=" + artifact,
                "-Dtransitive=false",
                "-q")
                .directory(dir)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        try {
            Process process = pb.start();
            try {
                if (!process.waitFor(120, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    LOG.warnf("Maven dependency:get timed out for %s — documentation search may be limited", artifact);
                    return false;
                }
                if (process.exitValue() == 0) {
                    LOG.infof("Successfully fetched RAG artifact %s via Maven", artifact);
                    return true;
                }
                LOG.warnf("Maven dependency:get failed for %s (exit code %d) — RAG data for this extension will be unavailable",
                        artifact, process.exitValue());
                return false;
            } finally {
                process.destroyForcibly();
            }
        } catch (IOException e) {
            LOG.warnf("Failed to start Maven for dependency:get (%s): %s", artifact, e.getMessage());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.debugf("Maven dependency:get interrupted for %s", artifact);
            return false;
        }
    }

    /**
     * Fixes metadata in non-core extension SQL fragments. The upstream plugin generates
     * metadata assuming core Quarkus conventions; this method corrects it at load time:
     * <ul>
     *   <li>{@code source} — replaced with the correct runtime artifact ID</li>
     *   <li>{@code quarkus_version} — renamed to {@code extension_version}; actual Quarkus version injected</li>
     *   <li>{@code url} — replaced with the guide URL from {@code quarkus-extension.yaml}, or removed if wrong</li>
     *   <li>{@code extension} — added (existing behavior)</li>
     * </ul>
     */
    static RagFragment injectExtensionMetadata(RagFragment fragment, String extensionName,
            String quarkusVersion, String guideUrl) {
        String sql = fragment.sql();

        // Fix source in DELETE statement
        sql = SOURCE_PATTERN.matcher(sql).replaceAll(
                Matcher.quoteReplacement("metadata->>'source' = '" + extensionName + "'"));

        // Fix source value in JSON metadata (source is always the first field)
        sql = sql.replaceAll("'\\{\"source\":\"[^\"]+\"",
                Matcher.quoteReplacement("'{\"source\":\"" + extensionName + "\""));

        // Add extension field before source
        sql = sql.replace("'{\"source\":",
                "'{\"extension\":\"" + extensionName + "\",\"source\":");

        // Rename version key to extension_version and inject correct quarkus_version.
        // Handles both old plugin format ("quarkus_version":) and new format (,"version":).
        if (quarkusVersion != null) {
            sql = sql.replace("\"quarkus_version\":", "\"extension_version\":");
            sql = sql.replace(",\"version\":", ",\"extension_version\":");
            sql = sql.replace("\"extension_version\":",
                    "\"quarkus_version\":\"" + quarkusVersion + "\",\"extension_version\":");
        }

        // Fix URL: use guide URL from extension metadata, or remove wrong quarkus.io URLs
        if (guideUrl != null) {
            sql = sql.replaceAll("\"url\":\"[^\"]*\"",
                    Matcher.quoteReplacement("\"url\":\"" + guideUrl + "\""));
        } else {
            sql = sql.replaceAll(",\"url\":\"https://quarkus\\.io/guides/[^\"]*\"", "");
        }

        return new RagFragment(extensionName, sql);
    }

    private String readGuideUrl(Path m2Repo, DependencyResolver.Dependency dep) {
        String groupPath = dep.groupId().replace('.', '/');
        Path runtimeJar = m2Repo.resolve(groupPath)
                .resolve(dep.artifactId())
                .resolve(dep.version())
                .resolve(dep.artifactId() + "-" + dep.version() + ".jar");
        if (!Files.isRegularFile(runtimeJar)) {
            return null;
        }
        try (JarFile jar = new JarFile(runtimeJar.toFile())) {
            SkillReader.ExtensionMetadata meta = SkillReader.readExtensionMetadata(jar);
            return meta != null ? meta.guide : null;
        } catch (IOException e) {
            LOG.debugf("Failed to read guide URL from %s: %s", runtimeJar, e.getMessage());
            return null;
        }
    }

    private RagFragment injectExtensionFromSource(RagFragment fragment) {
        // Single-pass transform instead of two chained String.replaceAll()/replace() calls.
        // The aggregated core-docs SQL fragment can be tens of MB (e.g. ~47MB for the current
        // Quarkus documentation artifact); doing two full-string regex/replace passes each
        // materializes a fresh copy of the whole string, which can spike memory well past a
        // constrained heap (observed to OOM with -Xmx512m). Here we scan once and copy each
        // "between match" chunk straight into a pre-sized StringBuilder, applying the literal
        // ",\"version\":" -> ",\"quarkus_version\":" fix-up per chunk instead of on the whole
        // string, so peak memory stays close to ~1x the input size rather than ~3-4x.
        String sql = fragment.sql();
        Matcher matcher = EXTENSION_FROM_SOURCE_PATTERN.matcher(sql);
        StringBuilder result = new StringBuilder(sql.length() + 64);
        int lastEnd = 0;
        while (matcher.find()) {
            appendWithVersionKeyFix(result, sql, lastEnd, matcher.start());
            String source = matcher.group(1);
            result.append("'{\"extension\":\"").append(source).append("\",\"source\":\"").append(source).append('"');
            lastEnd = matcher.end();
        }
        appendWithVersionKeyFix(result, sql, lastEnd, sql.length());
        return new RagFragment(fragment.source(), result.toString());
    }

    /**
     * Appends sql[from, to) to result, rewriting the generic "version" JSON key to
     * "quarkus_version" for core extensions, without allocating an intermediate copy
     * of the (potentially very large) full string.
     */
    private static void appendWithVersionKeyFix(StringBuilder result, String sql, int from, int to) {
        int searchFrom = from;
        int idx;
        while ((idx = sql.indexOf(VERSION_KEY, searchFrom)) != -1 && idx < to) {
            result.append(sql, searchFrom, idx).append(QUARKUS_VERSION_KEY);
            searchFrom = idx + VERSION_KEY.length();
        }
        result.append(sql, searchFrom, to);
    }

    private Path resolveAggregatedJarPath(String version, Path m2Repo) {
        return m2Repo.resolve(AGGREGATED_GROUP_PATH)
                .resolve(AGGREGATED_ARTIFACT_ID)
                .resolve(version)
                .resolve(AGGREGATED_ARTIFACT_ID + "-" + version + ".jar");
    }

    private Path downloadFromMavenCentral(String version, Path targetPath) {
        return downloadArtifact(AGGREGATED_GROUP_PATH, AGGREGATED_ARTIFACT_ID,
                version, targetPath, null);
    }

    private Path downloadArtifact(String groupPath, String artifactId,
            String version, Path targetPath, String projectDir) {
        if (version.endsWith("-SNAPSHOT")) {
            LOG.debugf("Skipping remote download for SNAPSHOT version %s", version);
            return null;
        }

        SkillReader.MavenRepoInfo repoInfo = SkillReader.resolveMavenRepoInfo(projectDir);
        String artifactUrlPath = "/" + groupPath + "/" + artifactId
                + "/" + version
                + "/" + artifactId + "-" + version + ".jar";
        String url = repoInfo.url() + artifactUrlPath;

        LOG.infof("RAG SQL not found locally, downloading from %s...", url);

        try {
            var request = webClient.getAbs(url).timeout(60_000);
            SkillReader.addAuthHeader(request, repoInfo, projectDir);

            var response = request.send().await().atMost(Duration.ofSeconds(65));

            if (response.statusCode() == 200) {
                Files.createDirectories(targetPath.getParent());
                Files.write(targetPath, response.body().getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                LOG.infof("Downloaded RAG SQL artifact to %s", targetPath);
                return targetPath;
            } else {
                LOG.warnf("RAG SQL artifact not available at %s (HTTP %d) — documentation search will be limited",
                        url, response.statusCode());
                return null;
            }
        } catch (IOException | RuntimeException e) {
            LOG.warnf("Failed to download RAG SQL from %s: %s", url, e.getMessage());
            return null;
        }
    }

    private List<RagFragment> scanCoreExtensionJars(Path m2Repo, String version) {
        Path quarkusDir = m2Repo.resolve("io/quarkus");
        if (!Files.isDirectory(quarkusDir)) {
            return List.of();
        }

        List<RagFragment> fragments = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(quarkusDir,
                entry -> Files.isDirectory(entry)
                        && entry.getFileName().toString().startsWith("quarkus-")
                        && entry.getFileName().toString().endsWith(DEPLOYMENT_SUFFIX))) {
            for (Path extDir : stream) {
                String deploymentArtifactId = extDir.getFileName().toString();
                String artifactId = deploymentArtifactId.substring(0,
                        deploymentArtifactId.length() - DEPLOYMENT_SUFFIX.length());
                Path deploymentJar = extDir.resolve(version)
                        .resolve(deploymentArtifactId + "-" + version + ".jar");

                if (!Files.isRegularFile(deploymentJar)) {
                    continue;
                }

                RagFragment fragment = readFragmentFromJar(deploymentJar, artifactId);
                if (fragment != null) {
                    fragments.add(injectExtensionFromSource(fragment));
                    LOG.debugf("Found RAG SQL in %s", deploymentArtifactId);
                }
            }
        } catch (IOException e) {
            LOG.debugf("Failed to scan extension JARs: %s", e.getMessage());
        }

        return fragments;
    }

    private RagFragment readFragmentFromJar(Path jarPath, String fallbackSource) {
        if (!Files.isRegularFile(jarPath)) {
            return null;
        }
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry entry = jar.getJarEntry(RAG_DATA_SQL_PATH);
            if (entry == null) {
                entry = jar.getJarEntry(RAG_SQL_PATH);
            }
            if (entry == null) {
                return null;
            }

            String sql;
            try (InputStream is = jar.getInputStream(entry)) {
                sql = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
            String source = extractSource(sql, fallbackSource);
            return new RagFragment(source, sql);
        } catch (IOException e) {
            LOG.debugf("Failed to read RAG SQL from %s: %s", jarPath, e.getMessage());
            return null;
        }
    }

    static String extractSource(String sql, String fallbackSource) {
        Matcher rowMatcher = ROW_SOURCE_PATTERN.matcher(sql);
        if (rowMatcher.find()) {
            return rowMatcher.group(1);
        }
        Matcher m = SOURCE_PATTERN.matcher(sql);
        if (m.find()) {
            return m.group(1);
        }
        return fallbackSource;
    }

    /**
     * Like {@link #readFragmentFromJar}, but never materializes the (potentially very large)
     * SQL content in memory: it only reads far enough to determine the fragment's {@code source}
     * name (a handful of lines, in practice), and returns a fragment that streams its actual
     * content later, directly from the jar, in {@link #streamFragmentIntoDb}.
     */
    private RagFragment peekStreamedFragment(Path jarPath, String fallbackSource) {
        if (!Files.isRegularFile(jarPath)) {
            return null;
        }
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry entry = jar.getJarEntry(RAG_DATA_SQL_PATH);
            if (entry == null) {
                entry = jar.getJarEntry(RAG_SQL_PATH);
            }
            if (entry == null) {
                return null;
            }
            String source = fallbackSource;
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(jar.getInputStream(entry), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher rowMatcher = ROW_SOURCE_PATTERN.matcher(line);
                    if (rowMatcher.find()) {
                        source = rowMatcher.group(1);
                        break;
                    }
                    Matcher m = SOURCE_PATTERN.matcher(line);
                    if (m.find()) {
                        source = m.group(1);
                        break;
                    }
                }
            }
            return new RagFragment(source, null, jarPath);
        } catch (IOException e) {
            LOG.debugf("Failed to peek RAG SQL source from %s: %s", jarPath, e.getMessage());
            return null;
        }
    }

    @FunctionalInterface
    private interface SqlStatementSink {
        void accept(String statement) throws SQLException;
    }

    /**
     * Reads a {@code fragment.streamJarPath()}'s {@code META-INF/quarkus-rag(-data).sql} entry
     * and executes each statement directly against the database as it is parsed, transforming
     * one (small) statement at a time instead of the whole file. This keeps peak memory usage
     * proportional to a single row (a few KB) rather than the size of the whole aggregated
     * artifact (tens of MB), which previously caused OutOfMemoryError with a constrained heap.
     */
    private long streamFragmentIntoDb(Statement stmt, RagFragment fragment) throws SQLException {
        Path jarPath = fragment.streamJarPath();
        long[] count = { 0 };
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            JarEntry entry = jar.getJarEntry(RAG_DATA_SQL_PATH);
            if (entry == null) {
                entry = jar.getJarEntry(RAG_SQL_PATH);
            }
            if (entry == null) {
                LOG.warnf("RAG SQL entry not found for streamed fragment %s", fragment.source());
                return 0;
            }
            try (PushbackReader reader = new PushbackReader(
                    new BufferedReader(new InputStreamReader(jar.getInputStream(entry), StandardCharsets.UTF_8),
                            1 << 16),
                    1)) {
                streamSplitAndConsume(reader, statement -> {
                    stmt.execute(transformStatement(statement));
                    count[0]++;
                });
            }
        } catch (IOException e) {
            throw new SQLException("Failed to stream RAG SQL from " + jarPath, e);
        }
        return count[0];
    }

    /**
     * Applies the same two transforms as {@link #injectExtensionFromSource} (inject an
     * "extension" JSON key derived from "source", and rename "version" to "quarkus_version"),
     * but to a single (small) SQL statement rather than to a whole multi-MB file.
     */
    private static String transformStatement(String statement) {
        Matcher matcher = EXTENSION_FROM_SOURCE_PATTERN.matcher(statement);
        String withExtension;
        if (matcher.find()) {
            StringBuilder sb = new StringBuilder(statement.length() + 64);
            int lastEnd = 0;
            matcher.reset();
            while (matcher.find()) {
                sb.append(statement, lastEnd, matcher.start());
                String source = matcher.group(1);
                sb.append("'{\"extension\":\"").append(source).append("\",\"source\":\"").append(source).append('"');
                lastEnd = matcher.end();
            }
            sb.append(statement, lastEnd, statement.length());
            withExtension = sb.toString();
        } else {
            withExtension = statement;
        }
        return withExtension.replace(VERSION_KEY, QUARKUS_VERSION_KEY);
    }

    /**
     * Streaming equivalent of {@link #splitSqlStatements(String)}: reads one character at a
     * time from {@code reader} instead of from an in-memory String, invoking {@code sink} with
     * each statement as soon as it's complete. The rolling buffer only ever holds a single
     * statement, so memory usage is bounded by the largest individual statement rather than
     * the size of the whole input.
     */
    private static void streamSplitAndConsume(PushbackReader reader, SqlStatementSink sink)
            throws IOException, SQLException {
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inLineComment = false;
        int ic;

        while ((ic = reader.read()) != -1) {
            char c = (char) ic;

            if (c == '\n') {
                inLineComment = false;
                current.append(c);
                continue;
            }

            if (inLineComment) {
                continue;
            }

            if (c == '-' && !inSingleQuote) {
                int next = reader.read();
                if (next == '-') {
                    inLineComment = true;
                    continue;
                }
                if (next != -1) {
                    reader.unread(next);
                }
            }

            if (c == '\'') {
                if (inSingleQuote) {
                    int next = reader.read();
                    if (next == '\'') {
                        current.append('\'').append('\'');
                        continue;
                    }
                    if (next != -1) {
                        reader.unread(next);
                    }
                }
                inSingleQuote = !inSingleQuote;
            }

            if (c == ';' && !inSingleQuote) {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    sink.accept(stmt);
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        String remaining = current.toString().trim();
        if (!remaining.isEmpty()) {
            sink.accept(remaining);
        }
    }

    private void ensureSchema(String jdbcUrl, String user, String password) {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_EXTENSION_DDL);
            stmt.execute(CREATE_TABLE_DDL);
        } catch (SQLException e) {
            LOG.warnf("Failed to create RAG schema: %s", e.getMessage());
        }
    }

    private Set<String> queryExistingSources(String jdbcUrl, String user, String password) {
        Set<String> sources = new HashSet<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement()) {
            // Table might not exist yet
            stmt.execute(CREATE_EXTENSION_DDL);
            stmt.execute(CREATE_TABLE_DDL);
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT DISTINCT metadata->>'source' FROM " + RAG_DOCUMENTS_TABLE)) {
                while (rs.next()) {
                    String source = rs.getString(1);
                    if (source != null) {
                        sources.add(source);
                    }
                }
            }
            if (!sources.isEmpty()) {
                LOG.infof("Container already has RAG data for %d source(s)", sources.size());
            }
        } catch (SQLException e) {
            LOG.debugf("Failed to query existing RAG sources: %s", e.getMessage());
        }
        return sources;
    }

    private void loadSql(String jdbcUrl, String user, String password,
            List<RagFragment> fragments, String version) {
        LOG.infof("Loading %d RAG SQL fragment(s) for Quarkus %s...", fragments.size(), version);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(CREATE_EXTENSION_DDL);
                stmt.execute(CREATE_TABLE_DDL);

                for (RagFragment fragment : fragments) {
                    if (fragment.streamJarPath() != null) {
                        long count = streamFragmentIntoDb(stmt, fragment);
                        LOG.debugf("Loaded RAG source: %s (%d statement(s), streamed)", fragment.source(), count);
                    } else {
                        for (String statement : splitSqlStatements(fragment.sql())) {
                            if (!statement.isBlank()) {
                                stmt.execute(statement);
                            }
                        }
                        LOG.debugf("Loaded RAG source: %s", fragment.source());
                    }
                }

                stmt.execute(CREATE_INDEX_DDL);
            }

            conn.commit();

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + RAG_DOCUMENTS_TABLE)) {
                if (rs.next()) {
                    LOG.infof("RAG data loaded: %d total documents for Quarkus %s", rs.getLong(1), version);
                }
            }
        } catch (SQLException e) {
            LOG.errorf(e, "Failed to load RAG SQL for Quarkus %s", version);
        }
    }

    /**
     * Splits a SQL string into individual statements.
     * Handles the fact that text values can contain semicolons.
     */
    static List<String> splitSqlStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inLineComment = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == '\n') {
                inLineComment = false;
                current.append(c);
                continue;
            }

            if (inLineComment) {
                continue;
            }

            if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-' && !inSingleQuote) {
                inLineComment = true;
                continue;
            }

            if (c == '\'') {
                if (inSingleQuote && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    current.append('\'');
                    current.append('\'');
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
            }

            if (c == ';' && !inSingleQuote) {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    statements.add(stmt);
                }
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        String remaining = current.toString().trim();
        if (!remaining.isEmpty()) {
            statements.add(remaining);
        }

        return statements;
    }

    private String detectLatestInstalledVersion() {
        Path quarkusDir = Path.of(System.getProperty("user.home"), ".m2", "repository", "io", "quarkus", "quarkus-core");
        if (!Files.isDirectory(quarkusDir)) {
            return null;
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(quarkusDir, Files::isDirectory)) {
            String latest = null;
            for (Path versionDir : stream) {
                String v = versionDir.getFileName().toString();
                if (!v.contains("SNAPSHOT") && (latest == null || v.compareTo(latest) > 0)) {
                    latest = v;
                }
            }
            return latest;
        } catch (IOException e) {
            return null;
        }
    }
}
