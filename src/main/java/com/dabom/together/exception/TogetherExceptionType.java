package com.dabom.together.exception;

import com.dabom.common.exception.ExceptionType;
import org.springframework.http.HttpStatus;

public enum TogetherExceptionType implements ExceptionType {
    NOT_VALID_CODE(HttpStatus.NOT_FOUND, "해당하는 Together 방이 존재하지 않습니다. 코드를 확인해주세요."),
    MAX_TOGETHER_MEMBER(HttpStatus.BAD_REQUEST, "해당하는 Together 방의 정원이 다 찼습니다. 다른 방을 찾아주세요."),
    NOT_MASTER_MEMBER(HttpStatus.FORBIDDEN, "방을 만든 멤버가 아닙니다."),
    CHOICE_OPEN_STATUS(HttpStatus.BAD_REQUEST, "방의 상태를 공개로 할 것인지, 비공개로 할 것인지 선택해주세요."),
    NOT_ACCEPT_MEMBER(HttpStatus.BAD_REQUEST, "허가받은 방이 아닙니다."),
    NOT_VALID_TOGETHER(HttpStatus.BAD_REQUEST, "허가받은 방이 아닙니다. 다시 시도해주세요.");

    private final HttpStatus httpStatus;
    private final String message;

    TogetherExceptionType(HttpStatus httpStatus, String message) {
        this.httpStatus = httpStatus;
        this.message = message;
    }

    @Override
    public HttpStatus statusCode() {
        return httpStatus;
    }

    @Override
    public String message() {
        return message;
    }
}
