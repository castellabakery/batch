package kr.co.pg.batch.reconcile.pg.k.step;

import kr.co.pg.batch.reconcile.common.step.listener.CommonStepLoggingListener;
import kr.co.pg.batch.reconcile.pg.k.KPgReconcileBatchConfig;
import kr.co.pg.batch.reconcile.pg.k.domain.KPgShopIdAndKeySet;
import kr.co.pg.batch.reconcile.pg.k.domain.entity.KPgReconcileOrigin;
import kr.co.pg.batch.reconcile.pg.k.enums.KPgReconcileEnum;
import kr.co.pg.batch.reconcile.pg.k.job.parameters.KPgReconcileJobParameters;
import kr.co.pg.batch.reconcile.pg.k.repository.KPgReconcileOriginRepository;
import kr.co.pg.batch.reconcile.pg.k.utils.KFileManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Configuration
public class KPgReconcileFilesReadStepConfig {

    private final String STEP_READ_LST_FILE = KPgReconcileBatchConfig.PREFIX + "ReadLstFileStep";
    private final String STEP_READ_EDI_FILE = KPgReconcileBatchConfig.PREFIX + "ReadEdiFileStep";

    private final String READER = KPgReconcileBatchConfig.PREFIX + "FileReader";

    private final String MULTI_READER = KPgReconcileBatchConfig.PREFIX + "MultiFileReader";

    private final StepBuilderFactory stepBuilderFactory;

    private final KFileManager kFileManager;

    private final KPgReconcileOriginRepository kPgReconcileOriginRepository;

    private final KPgReconcileJobParameters kPgReconcileJobParameters;

    private final String FILE_ENCODING_CHARSET = "EUC-KR";

    private final int CHUNK_SIZE = 100;

    private final CommonStepLoggingListener commonStepLoggingListener;

    private final String PG_RECONCILE_TYPE_LST = "0";
    private final String PG_RECONCILE_TYPE_EDI = "1";

    private final String CREDIT_CARD_COMMON = "1";
    private final String CREDIT_CARD_INTEREST_FREE = "2";


    @Autowired
    public KPgReconcileFilesReadStepConfig(StepBuilderFactory stepBuilderFactory, KFileManager kFileManager
            , KPgReconcileOriginRepository kPgReconcileOriginRepository
            , KPgReconcileJobParameters kPgReconcileJobParameters
            , CommonStepLoggingListener commonStepLoggingListener) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.kFileManager = kFileManager;
        this.kPgReconcileOriginRepository = kPgReconcileOriginRepository;
        this.kPgReconcileJobParameters = kPgReconcileJobParameters;
        this.commonStepLoggingListener = commonStepLoggingListener;
    }

    @Bean(name = STEP_READ_LST_FILE)
    @JobScope
    public Step readLst() {

        LocalDate standardDate = kPgReconcileJobParameters.getStandardDate();
        String adminId = kPgReconcileJobParameters.getAdminId();

        return stepBuilderFactory.get(STEP_READ_LST_FILE)
                .listener(commonStepLoggingListener)
                .<KPgReconcileOrigin, KPgReconcileOrigin>chunk(CHUNK_SIZE)
                .reader(multiResourceItemReader(KFileManager.SUPPORT_FILE_TYPE_PT, standardDate))
                .processor(fileReadProcessor(standardDate, adminId))
                .writer(fileToOriginWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();

    }

    @Bean(name = STEP_READ_EDI_FILE)
    @JobScope
    public Step readEdi() {

        LocalDate standardDate = kPgReconcileJobParameters.getStandardDate();
        String adminId = kPgReconcileJobParameters.getAdminId();

        return stepBuilderFactory.get(STEP_READ_EDI_FILE)
                .listener(commonStepLoggingListener)
                .<KPgReconcileOrigin, KPgReconcileOrigin>chunk(CHUNK_SIZE)
                .reader(multiResourceItemReader(KFileManager.SUPPORT_FILE_TYPE_PO, standardDate))
                .processor(fileReadProcessor(standardDate, adminId))
                .writer(fileToOriginWriter())
                .faultTolerant()
                .noSkip(Exception.class)
                .noRetry(Exception.class)
                .build();
    }

    private FlatFileItemReader fileReader(String fileType) {
        return new FlatFileItemReaderBuilder<KPgReconcileOrigin>()
                .name(READER)
                .encoding(FILE_ENCODING_CHARSET)
                .linesToSkip(1) // skip first line
                .strict(true)
                .lineMapper((line, lineNumber) -> {
                    if (line != null) {
                        if (line.indexOf("TT") == 0) {
                            return null;
                        }
                        return KPgReconcileOrigin.ByReadFile()
                                .data(line)
                                .reconcileType(KFileManager.SUPPORT_FILE_TYPE_PT.equalsIgnoreCase(fileType) ? PG_RECONCILE_TYPE_LST : PG_RECONCILE_TYPE_EDI)
                                .build();
                    }
                    return null;

                }).build();
    }

    private MultiResourceItemReader multiResourceItemReader(String fileType, LocalDate standardDate) {
        List<Resource> resources = new ArrayList<>();

        for (KPgShopIdAndKeySet kPgShopIdAndKeySet : kFileManager.getKPgShopIdAndKeySetList()) {
            resources.add(new FileSystemResource(kFileManager.getReconcileFileFullPath(fileType, standardDate, kPgShopIdAndKeySet.getShopId())));
        }

        return new MultiResourceItemReaderBuilder<KPgReconcileOrigin>()
                .name(MULTI_READER)
                .resources(resources.toArray(new Resource[resources.size()]))
                .delegate(fileReader(fileType))
                .build();
    }

    private ItemProcessor<KPgReconcileOrigin, KPgReconcileOrigin> fileReadProcessor(LocalDate standardDate, String adminId) {
        return item -> {
            KPgReconcileOrigin reconcileOrigin;

            String dataType;
            String interestType;
            try {
                dataType = new String(item.getData().getBytes(FILE_ENCODING_CHARSET),
                        KPgReconcileEnum.DATA_TYPE.getOffset(), KPgReconcileEnum.DATA_TYPE.getLength());
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to parse DATA_TYPE text - dataByte: ")
                        .append(item.getData()).append(", reason: ").append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            try {
                interestType = new String(item.getData().getBytes(FILE_ENCODING_CHARSET),
                        KPgReconcileEnum.INTEREST_TYPE.getOffset(), KPgReconcileEnum.INTEREST_TYPE.getLength());
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to parse INTEREST_TYPE text - dataByte: ")
                        .append(item.getData()).append(", reason: ").append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            if (!"DC".equals(dataType) || (CREDIT_CARD_COMMON.equals(interestType) || CREDIT_CARD_INTEREST_FREE.equals(interestType))) {
                log.info("Skipped data - {}", item);
                return null;
            }

            try {
                reconcileOrigin = KPgReconcileOrigin.ByInsert()
                        .data(item.getData())
                        .reconcileType(item.getReconcileType())
                        .fileDate(standardDate)
                        .createdId(adminId)
                        .build();
            } catch (Exception e) {
                String errorMessage = new StringBuilder("Failed to build entity to insert - ")
                        .append(item).append(", reason: ").append(e.getMessage()).toString();
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            return reconcileOrigin;
        };
    }

    private ItemWriter<KPgReconcileOrigin> fileToOriginWriter() {
        return new RepositoryItemWriterBuilder<KPgReconcileOrigin>()
                .repository(kPgReconcileOriginRepository)
                .build();
    }

}
