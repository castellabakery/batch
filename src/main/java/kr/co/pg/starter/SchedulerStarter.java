package kr.co.pg.starter;

import kr.co.pg.scheduler.domain.JobSchedulerVo;
import kr.co.pg.scheduler.service.JobSchedulerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

@Slf4j
@Component
public class SchedulerStarter {

    private static final boolean IS_STOPPED_APPLICATION_WHEN_FAIL = true;

    private final JobSchedulerService jobSchedulerService;

    private final Integer dbConfigNumber;

    private final String springProfile;

    @Autowired
    public SchedulerStarter(JobSchedulerService jobSchedulerService
            , @Value("${db-configuration}") Integer dbConfigNumber
            , @Value("${spring.config.activate.on-profile}") String springProfile) {
        this.jobSchedulerService = jobSchedulerService;
        this.dbConfigNumber = dbConfigNumber;
        this.springProfile = springProfile;
    }

    @PostConstruct
    public void start() throws Exception {
        log.info("Start Auto Batch Scheduler");

        List<JobSchedulerVo> initiatedScheduler = jobSchedulerService.init(IS_STOPPED_APPLICATION_WHEN_FAIL);
        log.info("initiated scheduler - total count: {}", initiatedScheduler.size());
        for (JobSchedulerVo vo : initiatedScheduler) {
            log.info("SCHEDULER: {}", vo);
        }

        this.checkDbConfigNumber();
    }

    private void checkDbConfigNumber() {

        if (dbConfigNumber == null) {
            log.error("db configuration number is null - dbConfigNumber : {}", dbConfigNumber);
            throw new IllegalArgumentException("db configuration number is null");
        }

        if (springProfile == null) {
            log.error("springProfile is null - springProfile : {}", springProfile);
            throw new IllegalArgumentException("springProfile is null");
        }

        if (("local".equals(springProfile) && dbConfigNumber != 0)
                || ("dev".equals(springProfile) && dbConfigNumber != 1)
                || ("prod".equals(springProfile) && dbConfigNumber != 2)
        ) {
            log.error("invalid db configuration option - springProfile : {}, dbConfigNumber : {}", springProfile, dbConfigNumber);
            throw new IllegalArgumentException("invalid db configuration option");
        }
    }
}
