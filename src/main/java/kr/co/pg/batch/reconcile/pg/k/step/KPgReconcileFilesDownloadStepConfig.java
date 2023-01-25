package kr.co.pg.batch.reconcile.pg.k.step;

import kr.co.pg.batch.reconcile.pg.k.KPgReconcileBatchConfig;
import kr.co.pg.batch.reconcile.pg.k.domain.KPgShopIdAndKeySet;
import kr.co.pg.batch.reconcile.pg.k.domain.entity.KPgReconcileFile;
import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.repository.KPgReconcileFileRepository;
import kr.co.pg.batch.reconcile.pg.k.utils.KFileManager;
import kr.co.pg.util.DateFormatUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;

@Slf4j
@Configuration
public class KPgReconcileFilesDownloadStepConfig {

    private final String STEP_DOWNLOAD_LST_FILE = KPgReconcileBatchConfig.PREFIX + "LstFileDownloadStep";
    private final String STEP_DOWNLOAD_EDI_FILE = KPgReconcileBatchConfig.PREFIX + "EdiFileDownloadStep";

    private final StepBuilderFactory stepBuilderFactory;

    private final KPgReconcileFileRepository kPgReconcileFileRepository;

    private final KFileManager kFileManager;

    private final KPgReconcileJobParameters kPgReconcileJobParameters;

    private final int FILE_DOWNLOAD_SUCCESS_CODE = 0;
    private static final int MAX_RETRY_COUNT = 2;
    private static int retryCount = 0;


    @Autowired
    public KPgReconcileFilesDownloadStepConfig(StepBuilderFactory stepBuilderFactory
            , KPgReconcileFileRepository kPgReconcileFileRepository, KFileManager kFileManager
            , KPgReconcileJobParameters kPgReconcileJobParameters) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.kPgReconcileFileRepository = kPgReconcileFileRepository;
        this.kFileManager = kFileManager;
        this.kPgReconcileJobParameters = kPgReconcileJobParameters;
    }

    @Bean(name = STEP_DOWNLOAD_LST_FILE)
    @JobScope
    public Step downloadLst() {
        Step step = stepBuilderFactory.get(STEP_DOWNLOAD_LST_FILE)
                .tasklet(downloadLstFile())
                .build();
        return step;
    }

    @Bean(name = STEP_DOWNLOAD_EDI_FILE)
    @JobScope
    public Step downloadEdi() {
        Step step = stepBuilderFactory.get(STEP_DOWNLOAD_EDI_FILE)
                .tasklet(downloadEdiFile())
                .build();
        return step;
    }

    @Bean
    @StepScope
    public Tasklet downloadLstFile() {
        return tasklet(KFileManager.SUPPORT_FILE_TYPE_PO);
    }

    @Bean
    @StepScope
    public Tasklet downloadEdiFile() {
        return tasklet(KFileManager.SUPPORT_FILE_TYPE_PT);
    }

    private Tasklet tasklet(String fileType) {
        return (contribution, chunkContext) -> {

            LocalDate standardDate = kPgReconcileJobParameters.getStandardDate();
            boolean isFailed = false;

            try {
                for (KPgShopIdAndKeySet kPgShopIdAndKeySet : kFileManager.getKPgShopIdAndKeySetList()) {
                    requestFileDownloadApi(fileType.toUpperCase(), standardDate, kPgShopIdAndKeySet);
                }
            } catch (Exception e) {
                log.error("Failed to download pg file for k - file type: {}, file date: {}, reason: {}", fileType, standardDate, e.getMessage());
                isFailed = true;
            }

            if (isFailed) {

                if (retryCount < MAX_RETRY_COUNT) { // 최대 2회 재시도
                    log.info("Retry to download reconcile files - retry count: {}", ++retryCount);
                    return RepeatStatus.CONTINUABLE;
                }

                retryCount = 0;
                contribution.setExitStatus(ExitStatus.FAILED);
                log.error("The number of retries was exceeded");
                throw new Exception("The number of retries was exceeded");
            }

            return RepeatStatus.FINISHED;
        };
    }

    private void requestFileDownloadApi(String fileType, LocalDate standardDate, KPgShopIdAndKeySet kPgShopIdAndKeySet) throws Exception {

        KFileManager m = kFileManager;
        String fullPath = m.getReconcileFileFullPath(fileType, standardDate, kPgShopIdAndKeySet.getShopId());
//        int resultCode = .fileDownload(m.getHost(), m.getPort(), fullPath, fileType, m.getDownloadType(), kPgShopIdAndKeySet.getShopId(), kPgShopIdAndKeySet.getKey(), standardDate.format(DateFormatUtils.FULL_DATE));
        int resultCode = 0;

        String resultMessage = (resultCode == FILE_DOWNLOAD_SUCCESS_CODE) ? "성공" : "실패";
        log.info("request to download pg file for k - response result: {}, {}, host: {}, port: {}, fullPath: {}, pgType: {}, type: {}, shopId: {}, key: {}, standardDate: {}"
                , resultCode, resultMessage, m.getHost(), m.getPort(), fullPath, fileType, m.getDownloadType(), kPgShopIdAndKeySet.getShopId(), kPgShopIdAndKeySet.getKey(), standardDate);

        KPgReconcileFile file = KPgReconcileFile.ByInsert()
                .fileType(fileType)
                .fileName(m.getFileName(fileType, standardDate, kPgShopIdAndKeySet.getShopId()))
                .fileExtension(KFileManager.DEFAULT_EXTENSION)
                .fileDate(standardDate)
                .filePath(m.getOriginPath())
                .createdId(kPgReconcileJobParameters.getAdminId())
                .result(resultCode)
                .build();

        try {
            kPgReconcileFileRepository.save(file);
        } catch (Exception e) {
            log.error("Failed to insert response for file download - {}", file);
        }

        if (resultCode != FILE_DOWNLOAD_SUCCESS_CODE) {
            log.error("Incorrect result code for api - file type: {}, result code: {}", fileType, resultCode);
            throw new Exception("Incorrect result code for api - file type: " + fileType);
        }

    }

}
