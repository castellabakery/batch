package kr.co.pg.scheduler.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.Set;

@Slf4j
@Getter
@Component
class SchedulerHelper {

    private final boolean DEFAULT_SCHEDULE_REPLACE = true;
    private final Scheduler scheduler;

    @Autowired
    public SchedulerHelper(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void registerScheduler(JobDetail jobDetail, Trigger trigger) throws Exception {

        Assert.notNull(jobDetail, "The Job Detail must not be null.");
        Assert.notNull(trigger, "The trigger must not be null.");

        Set<Trigger> t = new HashSet<Trigger>();
        t.add(trigger);

        scheduler.scheduleJob(jobDetail, t, DEFAULT_SCHEDULE_REPLACE); // true 로 해야 replace 됨

    }

    public void unScheduler(String triggerKeyName) throws SchedulerException {
        log.info("Except Job Name - {}", triggerKeyName);
        scheduler.unscheduleJob(new TriggerKey(triggerKeyName));
    }

}
