package kr.co.pg.api.controller;

import kr.co.pg.api.domain.SchedulerApiResult;
import kr.co.pg.api.domain.dto.KReconcileDeleteApiRequestDto;
import kr.co.pg.api.domain.dto.KReconcileDeleteApiResponseDto;
import kr.co.pg.api.service.SchedulerApiService;
import kr.co.pg.api.domain.dto.SchedulerJobApiResponseDto;
import kr.co.pg.api.domain.dto.SchedulerJobApiRequestDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api")
public class SchedulerApiController {

    // todo api 호출 권한 체크 처리 추가 필요 (오픈 이후 적용)
    // todo job 활성/비활성 api 추가 필요 (개선)

    private final SchedulerApiService schedulerApiService;

    @Autowired
    public SchedulerApiController(SchedulerApiService schedulerApiService) {
        this.schedulerApiService = schedulerApiService;
    }

    @PostMapping("/job/execute")
    public ResponseEntity<SchedulerApiResult> executeJob(@RequestBody SchedulerJobApiRequestDto request) {
        log.info("API request for execution job - request: {}", request);

        SchedulerJobApiResponseDto response;
        try {
            response = schedulerApiService.executeJobExplicitly(request);
        } catch (Exception e) {
            log.error("Failed to execute job API - request: {}, reason: {}", request, e.getMessage());
            return ResponseEntity.ok(SchedulerApiResult.fail(e.getMessage()));
        }

        log.info("API response for execution job - response: {}", response);
        return ResponseEntity.ok(SchedulerApiResult.success(response));
    }

    @PostMapping("/job/parameters")
    public ResponseEntity<SchedulerApiResult> getJobParametersDetail(@RequestBody SchedulerJobApiRequestDto request) {
        log.info("API request for job parameters detail - request: {}", request);

        SchedulerJobApiResponseDto response;
        try {
            response = schedulerApiService.getJobParametersDetail(request);
        } catch (Exception e) {
            log.error("Failed to get job parameters - request: {}, reason: {}", request, e.getMessage());
            return ResponseEntity.ok(SchedulerApiResult.fail(e.getMessage()));
        }

        log.info("API response for job parameters detail - response: {}", response);
        return ResponseEntity.ok(SchedulerApiResult.success(response));
    }

    @PostMapping("/delete/k/pg/reconcile")
    public ResponseEntity<SchedulerApiResult> deleteAllKPgReconcileData(@RequestBody KReconcileDeleteApiRequestDto request) {
        log.info("API request for deleting k pg reconcile data - request: {}", request);

        KReconcileDeleteApiResponseDto response;
        try {
            response = schedulerApiService.deleteAllKReconcileData(request, true);
        } catch (Exception e) {
            log.error("Failed to delete k pg reconcile data - request: {}, reason: {}", request, e.getMessage());
            return ResponseEntity.ok(SchedulerApiResult.fail(e.getMessage()));
        }

        log.info("API response for deleting k pg reconcile data - response: {}", response);
        return ResponseEntity.ok(SchedulerApiResult.success(response));
    }

    @PostMapping("/delete/k/van/reconcile")
    public ResponseEntity<SchedulerApiResult> deleteAllKVanReconcileData(@RequestBody KReconcileDeleteApiRequestDto request) {
        log.info("API request for deleting k van reconcile data - request: {}", request);

        KReconcileDeleteApiResponseDto response;
        try {
            response = schedulerApiService.deleteAllKReconcileData(request, false);
        } catch (Exception e) {
            log.error("Failed to delete k van reconcile data - request: {}, reason: {}", request, e.getMessage());
            return ResponseEntity.ok(SchedulerApiResult.fail(e.getMessage()));
        }

        log.info("API response for deleting k van reconcile data - response: {}", response);
        return ResponseEntity.ok(SchedulerApiResult.success(response));
    }

    @GetMapping("/health")
    public ResponseEntity<SchedulerApiResult> healthCheck() {
        log.info("API request for health check");

        return ResponseEntity.ok(SchedulerApiResult.success());
    }

}
