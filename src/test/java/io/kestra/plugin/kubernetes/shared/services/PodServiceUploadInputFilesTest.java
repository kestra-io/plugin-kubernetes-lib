package io.kestra.plugin.kubernetes.shared.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.slf4j.Logger;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.WorkingDir;

import io.fabric8.kubernetes.api.model.ContainerStateBuilder;
import io.fabric8.kubernetes.api.model.ContainerStateTerminatedBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.dsl.ContainerResource;
import io.fabric8.kubernetes.client.dsl.CopyOrReadable;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.TtyExecErrorable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link PodService#uploadInputFiles}, ported from the duplicated OSS ({@code AbstractPod}) and
 * EE ({@code Kubernetes} runner) copies so both consumers share a single tested implementation.
 */
class PodServiceUploadInputFilesTest {
    private static final String CONTAINER_WORKING_DIR = "/kestra/working-dir";
    private static final String INIT_FILES_CONTAINER_NAME = "init-files";

    /**
     * Builds a {@link RunContext} mock whose working directory resolves to {@code root}, and pre-creates
     * {@code root/working-dir} so {@link PodService#uploadMarker} (which always writes the ready marker
     * under {@link PodService#tempDir}) can create the local marker file, mirroring the {@code mkdir()}
     * both consumers perform during task/task-runner init before any upload happens.
     */
    private static RunContext runContext(Path root) throws IOException {
        RunContext runContext = Mockito.mock(RunContext.class);
        WorkingDir workingDir = Mockito.mock(WorkingDir.class);
        Mockito.when(runContext.workingDir()).thenReturn(workingDir);
        Mockito.when(workingDir.path()).thenReturn(root);
        Files.createDirectories(root.resolve("working-dir"));
        return runContext;
    }

    @Test
    void shouldUploadInputFiles(@TempDir Path localBaseDir) throws Exception {
        PodResource podResource = Mockito.mock(PodResource.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);
        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(0)).thenReturn(container);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        // The ready marker still uploads via upload(Path); standalone input files upload via upload(InputStream).
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);

        Files.writeString(localBaseDir.resolve("a.txt"), "AAA");
        Files.writeString(localBaseDir.resolve("b.txt"), "BBB");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("a.txt"), Path.of("b.txt"));

        PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths);

        // Pins the fix for #329: init-files uploads must skip the pod-Ready wait, since the pod
        // structurally cannot become Ready while init-files itself is blocked on the ready marker.
        // Called once for the input-file uploads and once more inside PodService.uploadMarker.
        Mockito.verify(container, Mockito.times(2)).withReadyWaitTimeout(0);

        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/a.txt");
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/b.txt");

        // The 2 standalone input files stream via a single cat-style upload(InputStream)...
        Mockito.verify(fileUploader, Mockito.times(2)).upload(Mockito.any(InputStream.class));
        // ...while only the ready marker uses the tar-based upload(Path).
        Mockito.verify(fileUploader, Mockito.times(1)).upload(Mockito.any(Path.class));
    }

    @Test
    void shouldTolerateMarkerUploadFailureWhenInitContainerSucceeded(@TempDir Path localBaseDir) throws Exception {
        // Regression test: fabric8's exec WebSocket can close before reporting a clean result even though
        // the init container already consumed the ready marker and exited. That must be tolerated, not
        // surfaced as a task failure.
        PodResource podResource = Mockito.mock(PodResource.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(0)).thenReturn(container);

        ContainerStatus initFilesStatus = new ContainerStatusBuilder()
            .withName(INIT_FILES_CONTAINER_NAME)
            .withState(new ContainerStateBuilder()
                .withTerminated(new ContainerStateTerminatedBuilder().withExitCode(0).build())
                .build())
            .build();
        Pod terminatedPod = new PodBuilder()
            .withNewStatus()
                .withInitContainerStatuses(initFilesStatus)
            .endStatus()
            .build();
        Mockito.when(podResource.get()).thenReturn(terminatedPod);

        RunContext runContext = runContext(localBaseDir);

        try (var staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).thenThrow(new IOException("exec WebSocket closed before result"));

            assertDoesNotThrow(() ->
                PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, List.of())
            );
        }
    }

    @Test
    void shouldPropagateMarkerUploadFailureWhenInitContainerDidNotSucceed(@TempDir Path localBaseDir) throws Exception {
        PodResource podResource = Mockito.mock(PodResource.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(0)).thenReturn(container);

        ContainerStatus initFilesStatus = new ContainerStatusBuilder()
            .withName(INIT_FILES_CONTAINER_NAME)
            .withState(new ContainerStateBuilder()
                .withTerminated(new ContainerStateTerminatedBuilder().withExitCode(1).build())
                .build())
            .build();
        Pod failedPod = new PodBuilder()
            .withNewStatus()
                .withInitContainerStatuses(initFilesStatus)
            .endStatus()
            .build();
        Mockito.when(podResource.get()).thenReturn(failedPod);

        RunContext runContext = runContext(localBaseDir);

        try (var staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).thenThrow(new IOException("exec WebSocket closed before result"));

            assertThrows(IOException.class, () ->
                PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, List.of())
            );
        }
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldFallBackToPerFileUploadWhenBulkVerificationDetectsTruncatedTransfer(@TempDir Path localBaseDir) throws Exception {
        // Regression test: fabric8's dir().upload() can report success even when the tar transfer was
        // truncated (e.g. a Python dependency directory silently missing files). Post-upload verification
        // must catch the mismatch and fall back to re-uploading every file individually.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        // Both the raw upload and the verification-only exec (see PodService#execOutput) pass a 0
        // readyWaitTimeout and reuse this same container mock, so keep the lenient matcher here: the
        // exact value is pinned by shouldUploadInputFiles instead.
        Mockito.when(container.withReadyWaitTimeout(Mockito.any(Integer.class))).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        // fabric8 falsely reports success even though the transfer was truncated
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);
        // The ready marker still uploads via upload(Path).
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        // Simulate the verification exec: the directory file-count check reports only 1 file (truncated),
        // while the per-file size checks that follow during the fallback report the correct byte counts.
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                // Mockito expands varargs into individual arguments for InvocationOnMock, regardless of
                // how the real call packed them, so read them via getArguments() rather than getArgument(0).
                Object[] command = execInvocation.getArguments();
                String shellCommand = (String) command[command.length - 1];
                String response = shellCommand.contains("find")
                    ? "1"
                    : shellCommand.contains("pkg1.txt") ? "2" : "3";
                out.write(response.getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        Files.createDirectories(localBaseDir.resolve("deps"));
        Files.writeString(localBaseDir.resolve("deps/pkg1.txt"), "AA");
        Files.writeString(localBaseDir.resolve("deps/pkg2.txt"), "BBB");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("deps/pkg1.txt"), Path.of("deps/pkg2.txt"));

        PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths);

        // Bulk directory upload was attempted first...
        Mockito.verify(container, Mockito.times(1)).dir("/kestra/working-dir/deps");
        Mockito.verify(dirUploader, Mockito.times(1)).upload(localBaseDir.resolve("deps"));

        // ...but verification detected the truncated transfer, so every file was re-uploaded individually.
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/deps/pkg1.txt");
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/deps/pkg2.txt");
        // Standalone per-file fallback uploads stream via upload(InputStream), not upload(Path).
        Mockito.verify(fileUploader, Mockito.times(2)).upload(Mockito.any(InputStream.class));
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldAcceptBulkUploadWhenVerificationCountsMatch(@TempDir Path localBaseDir) throws Exception {
        // Happy-path companion to shouldFallBackToPerFileUploadWhenBulkVerificationDetectsTruncatedTransfer:
        // the pod-side file count matches the local directory, so verification passes and no per-file
        // fallback upload happens.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(Mockito.any(Integer.class))).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        // Realistic verification exec: the pod-side file count matches the two files uploaded locally.
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                out.write("2".getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        Files.createDirectories(localBaseDir.resolve("deps"));
        Files.writeString(localBaseDir.resolve("deps/pkg1.txt"), "AA");
        Files.writeString(localBaseDir.resolve("deps/pkg2.txt"), "BBB");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("deps/pkg1.txt"), Path.of("deps/pkg2.txt"));

        PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths);

        // Bulk directory upload was attempted, and the verification exec actually ran...
        Mockito.verify(container, Mockito.times(1)).dir("/kestra/working-dir/deps");
        Mockito.verify(dirUploader, Mockito.times(1)).upload(localBaseDir.resolve("deps"));
        Mockito.verify(container, Mockito.atLeastOnce()).writingOutput(Mockito.any(OutputStream.class));

        // ...counts matched, so no per-file fallback upload was needed.
        Mockito.verify(fileUploader, Mockito.never()).upload(Mockito.any(InputStream.class));
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldAcceptBulkUploadWhenPodReportsMoreEntriesThanExpected(@TempDir Path localBaseDir) throws Exception {
        // Regression test: a filename containing a newline used to make the pod-side 'find | wc -l' count
        // one file several times, so the check reported a truncated transfer on a perfectly good upload.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(Mockito.any(Integer.class))).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        AtomicReference<String> directoryCheck = new AtomicReference<>();

        // Two files locally, but the pod reports six — the shape a line-counting check produced.
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                Object[] command = execInvocation.getArguments();
                String shellCommand = (String) command[command.length - 1];
                if (shellCommand.contains("find")) {
                    directoryCheck.set(shellCommand);
                }
                out.write("6".getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        Files.createDirectories(localBaseDir.resolve("deps"));
        Files.writeString(localBaseDir.resolve("deps/pkg1.txt"), "AA");
        Files.writeString(localBaseDir.resolve("deps/\n\n--- Changes ---\n\n"), "BBB");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("deps/pkg1.txt"), Path.of("deps/\n\n--- Changes ---\n\n"));

        PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths);

        // An excess count is not a truncation, so the bulk upload stands and nothing is re-uploaded.
        Mockito.verify(dirUploader, Mockito.times(1)).upload(localBaseDir.resolve("deps"));
        Mockito.verify(container, Mockito.never()).file(Mockito.startsWith("/kestra/working-dir"));

        // The count itself must be NUL-separated, otherwise a newline in a filename inflates it.
        assertThat(directoryCheck.get(), containsString("-print0"));
        assertThat(directoryCheck.get(), not(containsString("wc -l")));
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldBulkUploadDirectory(@TempDir Path localBaseDir) throws Exception {
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(Mockito.anyInt())).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);
        // The ready marker still uploads via upload(Path).
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        Files.createDirectories(localBaseDir.resolve("data"));
        Files.writeString(localBaseDir.resolve("data/a.txt"), "A");
        Files.writeString(localBaseDir.resolve("data/b.txt"), "B");
        Files.writeString(localBaseDir.resolve("config.yaml"), "cfg");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(
            Path.of("data/a.txt"),
            Path.of("data/b.txt"),
            Path.of("config.yaml")
        );

        PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths);

        Mockito.verify(container, Mockito.times(1)).dir(Mockito.contains("/kestra/working-dir/data"));
        Mockito.verify(dirUploader, Mockito.times(1)).upload(localBaseDir.resolve("data"));

        Mockito.verify(container, Mockito.times(1)).file(Mockito.contains("/kestra/working-dir/config.yaml"));
        // config.yaml is a standalone top-level file, so it streams via upload(InputStream).
        Mockito.verify(fileUploader, Mockito.times(1)).upload(Mockito.any(InputStream.class));

        // we verify no individual file uploads for the bulk-uploaded directory contents
        Mockito.verify(container, Mockito.never()).file(Mockito.contains("/kestra/working-dir/data"));
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldRetryBulkVerificationOnTransientMismatchBeforeFallingBackToPerFile(@TempDir Path localBaseDir) throws Exception {
        // Regression test: the bulk-path verification call must go through withVerificationRetries just
        // like the per-file fallback loop does, so a transient exec race doesn't force an unnecessary
        // per-file fallback for an upload that was actually complete.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(Mockito.anyInt())).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        // First verification exec under-reports the file count (transient race), the retried exec reports
        // the correct count.
        AtomicInteger execCallCount = new AtomicInteger(0);
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                String response = execCallCount.incrementAndGet() == 1 ? "1" : "2";
                out.write(response.getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        Files.createDirectories(localBaseDir.resolve("deps"));
        Files.writeString(localBaseDir.resolve("deps/pkg1.txt"), "AA");
        Files.writeString(localBaseDir.resolve("deps/pkg2.txt"), "BBB");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("deps/pkg1.txt"), Path.of("deps/pkg2.txt"));

        PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths);

        // Bulk directory upload happened exactly once, and the verification exec was retried once...
        Mockito.verify(container, Mockito.times(1)).dir(Mockito.contains("/kestra/working-dir/deps"));
        Mockito.verify(dirUploader, Mockito.times(1)).upload(localBaseDir.resolve("deps"));
        assertThat("the mismatched verification exec must have been retried", execCallCount.get(), is(2));

        // ...so the transient mismatch resolved on retry and no per-file fallback upload was needed.
        Mockito.verify(fileUploader, Mockito.never()).upload(Mockito.any(InputStream.class));
    }

    @Test
    void shouldRejectAbsoluteRelativePath(@TempDir Path localBaseDir) throws Exception {
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        RunContext runContext = runContext(localBaseDir);

        Path absolute = localBaseDir.resolve("a.txt");
        assertThrows(IllegalArgumentException.class, () ->
            PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, List.of(absolute))
        );
    }

    @Test
    void shouldRejectPathEscapingBaseDirectory(@TempDir Path localBaseDir) throws Exception {
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        RunContext runContext = runContext(localBaseDir);

        Path escaping = Path.of("../outside.txt");
        assertThrows(IllegalArgumentException.class, () ->
            PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, List.of(escaping))
        );
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldFailUploadWhenPerFileVerificationDetectsMissingFile(@TempDir Path localBaseDir) throws Exception {
        // Regression test: a missing pod-side file used to make the wc/if-else check exit non-zero,
        // which verifyUpload treats the same as "tool unsupported" and silently skips — letting a
        // truncated per-file fallback upload pass verification. The fixed shell command instead
        // echoes 0 for a missing file, which must be reported as a real shortfall.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(Mockito.any(Integer.class))).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);

        // Bulk directory verification under-reports (forcing per-file fallback); the per-file check for
        // pkg1.txt then reports 0 (missing on the pod), while pkg2.txt reports its correct byte count.
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                Object[] command = execInvocation.getArguments();
                String shellCommand = (String) command[command.length - 1];
                String response = shellCommand.contains("find")
                    ? "1"
                    : shellCommand.contains("pkg1.txt") ? "0" : "3";
                out.write(response.getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        Files.createDirectories(localBaseDir.resolve("deps"));
        Files.writeString(localBaseDir.resolve("deps/pkg1.txt"), "AA");
        Files.writeString(localBaseDir.resolve("deps/pkg2.txt"), "BBB");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("deps/pkg1.txt"), Path.of("deps/pkg2.txt"));

        assertThrows(IOException.class, () ->
            PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths)
        );
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldUploadNestedDirectoryEntryDuringPerFileFallback(@TempDir Path localBaseDir) throws Exception {
        // The per-file fallback loop can itself encounter a directory entry (EE callers walk the working
        // dir into a List<Path> that isn't limited to regular files), which must go through container.dir(...)
        // rather than container.file(...).
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(Mockito.any(Integer.class))).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);
        // The ready marker still uploads via upload(Path).
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        // Force the top-level 'deps' bulk verification to fail (triggering fallback); any other
        // verification exec (the nested 'sub' directory check, or the 'file.txt' size check encountered
        // during the fallback) reports a value large enough to pass, since this test is only about which
        // fabric8 DSL call the fallback loop uses for a directory entry, not the verification outcome.
        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            OutputStream out = writingOutputInvocation.getArgument(0);
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                Object[] command = execInvocation.getArguments();
                String shellCommand = (String) command[command.length - 1];
                String response = shellCommand.contains("'/kestra/working-dir/deps'") ? "0" : "999999";
                out.write(response.getBytes(StandardCharsets.UTF_8));

                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(0));
                return watch;
            });
            return errorable;
        });

        Files.createDirectories(localBaseDir.resolve("deps/sub"));
        Files.writeString(localBaseDir.resolve("deps/sub/nested.txt"), "N");
        Files.writeString(localBaseDir.resolve("deps/file.txt"), "F");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("deps/sub"), Path.of("deps/file.txt"));

        PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths);

        Mockito.verify(container, Mockito.times(1)).dir("/kestra/working-dir/deps/sub");
        Mockito.verify(dirUploader, Mockito.times(1)).upload(localBaseDir.resolve("deps/sub"));
        Mockito.verify(container, Mockito.times(1)).file("/kestra/working-dir/deps/file.txt");
    }

    @Timeout(value = 15, unit = TimeUnit.MINUTES)
    @Test
    void shouldSkipVerificationWhenSidecarLacksTool(@TempDir Path localBaseDir) throws Exception {
        // When the verification exec exits non-zero (e.g. the sidecar image has no 'find'/'wc'), the
        // check must be skipped rather than failing — the bulk upload stands with no per-file fallback.
        PodResource podResource = Mockito.mock(PodResource.class);
        Logger logger = Mockito.mock(Logger.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(Mockito.any(Integer.class))).thenReturn(container);

        CopyOrReadable dirUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.dir(Mockito.anyString())).thenReturn(dirUploader);
        Mockito.when(dirUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        CopyOrReadable fileUploader = Mockito.mock(CopyOrReadable.class);
        Mockito.when(container.file(Mockito.anyString())).thenReturn(fileUploader);
        Mockito.when(fileUploader.upload(Mockito.any(InputStream.class))).thenReturn(true);
        // The ready marker still uploads via upload(Path).
        Mockito.when(fileUploader.upload(Mockito.any(Path.class))).thenReturn(true);

        Mockito.when(container.writingOutput(Mockito.any(OutputStream.class))).thenAnswer(writingOutputInvocation ->
        {
            TtyExecErrorable errorable = Mockito.mock(TtyExecErrorable.class);
            Mockito.when(errorable.exec(Mockito.any(String[].class))).thenAnswer(execInvocation ->
            {
                ExecWatch watch = Mockito.mock(ExecWatch.class);
                Mockito.when(watch.exitCode()).thenReturn(CompletableFuture.completedFuture(127));
                return watch;
            });
            return errorable;
        });

        Files.createDirectories(localBaseDir.resolve("deps"));
        Files.writeString(localBaseDir.resolve("deps/pkg1.txt"), "AA");

        RunContext runContext = runContext(localBaseDir);
        List<Path> relativePaths = List.of(Path.of("deps/pkg1.txt"));

        assertDoesNotThrow(() ->
            PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, relativePaths)
        );

        Mockito.verify(dirUploader, Mockito.times(1)).upload(localBaseDir.resolve("deps"));
        Mockito.verify(container, Mockito.never()).file(Mockito.startsWith("/kestra/working-dir"));
    }

    @Test
    void shouldPropagateMarkerErrorWithLookupFailureSuppressed(@TempDir Path localBaseDir) throws Exception {
        // When the marker upload fails AND the fallback status lookup (podResource.get()) also fails,
        // the original marker IOException must propagate, with the lookup failure attached as suppressed
        // rather than replacing it.
        PodResource podResource = Mockito.mock(PodResource.class);
        ContainerResource container = Mockito.mock(ContainerResource.class);
        Logger logger = Mockito.mock(Logger.class);

        Mockito.when(podResource.inContainer(INIT_FILES_CONTAINER_NAME)).thenReturn(container);
        Mockito.when(container.withReadyWaitTimeout(0)).thenReturn(container);

        RuntimeException lookupError = new RuntimeException("api server unreachable");
        Mockito.when(podResource.get()).thenThrow(lookupError);

        RunContext runContext = runContext(localBaseDir);

        IOException markerError = new IOException("exec WebSocket closed before result");
        try (var staticMock = Mockito.mockStatic(PodService.class, Mockito.CALLS_REAL_METHODS)) {
            staticMock.when(
                () -> PodService.uploadMarker(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.anyString())
            ).thenThrow(markerError);

            IOException thrown = assertThrows(IOException.class, () ->
                PodService.uploadInputFiles(runContext, podResource, logger, localBaseDir, CONTAINER_WORKING_DIR, List.of())
            );

            assertThat(thrown, is(markerError));
            assertThat(thrown.getSuppressed().length, is(1));
            assertThat(thrown.getSuppressed()[0], is(lookupError));
        }
    }
}
