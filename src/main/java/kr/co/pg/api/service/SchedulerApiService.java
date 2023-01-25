package kr.co.pg.api.service;

import kr.co.pg.api.domain.dto.*;
import kr.co.pg.api.domain.entity.BatchForcedExecution;
import kr.co.pg.api.repository.BatchForcedExecutionRepository;
import kr.co.pg.batch.reconcile.pg.k.service.KPgReconcileDeleteService;
import kr.co.pg.batch.reconcile.van.k.service.KVanReconcileDeleteService;
import kr.co.pg.scheduler.domain.JobExecutionVo;
import kr.co.pg.scheduler.service.JobSchedulerService;
import kr.co.pg.util.DateFormatUtils;
import kr.co.pg.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class SchedulerApiService {

    private final JobSchedulerService jobSchedulerService;

    private final BatchForcedExecutionRepository batchForcedExecutionRepository;

    private final KPgReconcileDeleteService kPgReconcileDeleteService;

    private final KVanReconcileDeleteService kVanReconcileDeleteService;

    @Autowired
    public SchedulerApiService(JobSchedulerService jobSchedulerService, BatchForcedExecutionRepository batchForcedExecutionRepository
            , KPgReconcileDeleteService kPgReconcileDeleteService
            , KVanReconcileDeleteService kVanReconcileDeleteService) {
        this.jobSchedulerService = jobSchedulerService;
        this.batchForcedExecutionRepository = batchForcedExecutionRepository;
        this.kPgReconcileDeleteService = kPgReconcileDeleteService;
        this.kVanReconcileDeleteService = kVanReconcileDeleteService;
    }

    public SchedulerJobApiResponseDto executeJobExplicitly(SchedulerJobApiRequestDto request) throws Exception {

        validate(request);
        Assert.notNull(request.getJobParameters(), "jobParameters must not be null.");

        if (StringUtils.isBlank(request.getRequestAdminId())) {
            throw new NullPointerException("requestAdminId is null");
        }

        JobExecutionVo jobExecutionVo;
        try {
            jobExecutionVo = jobSchedulerService.executeJob(request.getJobName(), request.getJobParameters());
        } catch (Exception e) {
            log.error("Failed to execute job explicitly - request: {}", request);
            throw e;
        }

        BatchForcedExecution batchForcedExecution = BatchForcedExecution.builder()
                .jobExecutionId(jobExecutionVo.getJobExecutionId())
                .executionDatetime(LocalDateTime.now())
                .executionAdminId(request.getRequestAdminId())
                .build();
        try {
            batchForcedExecutionRepository.save(batchForcedExecution);
        } catch (Exception e) {
            log.error("Failed to log for forced batch execution - value: {}", batchForcedExecution);
            return new SchedulerJobApiResponseDto(request.getJobName(), request.getJobParameters(), false);
        }

        return new SchedulerJobApiResponseDto(request.getJobName(), request.getJobParameters(), true);
    }

    public SchedulerJobApiResponseDto getJobParametersDetail(SchedulerJobApiRequestDto request) throws Exception {

        validate(request);

        List<JobParametersDetailDto> jobParametersDetailList;
        try {
            jobParametersDetailList = jobSchedulerService.getJobParametersDetail(request.getJobName());
        } catch (Exception e) {
            log.error("Failed to execute job explicitly - request: {}", request);
            throw e;
        }

        SchedulerJobApiResponseDto response = new SchedulerJobApiResponseDto();
        response.setJobName(request.getJobName());
        response.setJobParametersDetailList(jobParametersDetailList);
        return response;
    }

    public KReconcileDeleteApiResponseDto deleteAllKReconcileData(KReconcileDeleteApiRequestDto request, boolean isPgType) throws Exception {

        String standardDateStr = request.getStandardDate();
        if (StringUtils.isBlank(standardDateStr)) {
            throw new NullPointerException("standardDate is null");
        }

        if (StringUtils.isBlank(request.getRequestAdminId())) {
            throw new NullPointerException("requestAdminId is null");
        }

        LocalDate standardDate;
        try {
            standardDate = LocalDate.parse(standardDateStr, DateFormatUtils.DASH_DATE);
        } catch (Exception e) {
            log.error("standardDate is invalid format (required: YYYY-MM-DD) - request: {}", request);
            throw new IllegalArgumentException("standardDate is invalid format");
        }

        KReconcileDeleteApiResponseDto response;
        try {

            if (isPgType) {
                response = deleteAllKPgReconcileData(standardDate);
            } else {
                response = deleteAllKVanReconcileData(standardDate);
            }

        } catch (Exception e) {
            log.error("Failed to delete k reconcile all data - {}, request: {}", (isPgType ? "PG" : "VAN"), request);
            throw e;
        }

        return response;
    }

    private KReconcileDeleteApiResponseDto deleteAllKPgReconcileData(LocalDate standardDate) throws Exception {

        Map<String, Long> deletedContent;
        try {
            deletedContent = kPgReconcileDeleteService.deleteAll(standardDate);
        } catch (Exception e) {
            throw e;
        }

        return KReconcileDeleteApiResponseDto.ByPg()
                .kPgReconcileOriginDeletedCount(deletedContent.get("kPgReconcileOriginDeletedCount"))
                .kPgReconcileDeletedCount(deletedContent.get("kPgReconcileDeletedCount"))
                .pgReconcileCompletedDeletedCount(deletedContent.get("pgReconcileCompletedDeletedCount"))
                .paymentMainDeletedCount(deletedContent.get("paymentMainDeletedCount"))
                .paymentDetailDeletedCount(deletedContent.get("paymentDetailDeletedCount"))
                .paymentAddinfoDeletedCount(deletedContent.get("paymentAddinfoDeletedCount"))
                .cardUdrsDeletedCount(deletedContent.get("cardUdrsDeletedCount"))
                .build();

    }

    private KReconcileDeleteApiResponseDto deleteAllKVanReconcileData(LocalDate standardDate) throws Exception {

        Map<String, Long> deletedContent;
        try {
            deletedContent = kVanReconcileDeleteService.deleteAll(standardDate);
        } catch (Exception e) {
            throw e;
        }

        return KReconcileDeleteApiResponseDto.ByVan()
                .vanReconcileCompletedDeletedCount(deletedContent.get("vanReconcileCompletedDeletedCount"))
                .paymentMainDeletedCount(deletedContent.get("paymentMainDeletedCount"))
                .paymentDetailDeletedCount(deletedContent.get("paymentDetailDeletedCount"))
                .paymentAddinfoDeletedCount(deletedContent.get("paymentAddinfoDeletedCount"))
                .cardUdrsDeletedCount(deletedContent.get("cardUdrsDeletedCount"))
                .build();
    }

    private void validate(SchedulerJobApiRequestDto request) {

        Assert.notNull(request, "request must not be null.");

        if (StringUtils.isBlank(request.getJobName())) {
            throw new NullPointerException("jobName is null");
        }

    }

}
