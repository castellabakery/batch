package kr.co.pg.batch;

import org.springframework.batch.core.JobParameters;

/**
 * 해당 인터페이스를 상속받아 구현하는 JobParameter에서는 멤버 필드에 대해 setter 를 구현해야 함
 * (api로 job 실행 시, map으로 파라미터를 전달받아 JobParameter 객체로 변환하는 작업이 있기 때문)
 */
public interface DefaultJobParameters {

    JobParameters toJobParameter();

    void validate() throws IllegalArgumentException;
}
