package kr.co.pg.mail.domain.entity;

import kr.co.pg.util.KEncrypt;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import javax.persistence.*;

@Getter
@Entity
@ToString
@NoArgsConstructor
@Table(name = "TB_ALARM_TARGET")
public class AlarmEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long seq;
    private String email;
    private String emailYn;
    private String phone;
    private String messageYn;
    private String useYn;

    public String getEmail() {
        try {
            return KEncrypt.decrypt(this.email);
        } catch (Exception e) {
            return email;
        }
    }
}
