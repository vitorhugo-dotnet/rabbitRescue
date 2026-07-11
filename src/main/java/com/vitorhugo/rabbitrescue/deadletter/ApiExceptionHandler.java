package com.vitorhugo.rabbitrescue.deadletter;

import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(DeadLetterNotFoundException.class)
    ResponseEntity<ProblemDetail> handleNotFound(DeadLetterNotFoundException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.NOT_FOUND,
                exception.getMessage()
        );
        problem.setTitle("Mensagem não encontrada na DLQ");
        problem.setType(URI.create("urn:rabbit-rescue:dead-letter-not-found"));
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(problem);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    ResponseEntity<ProblemDetail> handleBadRequest(IllegalArgumentException exception) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST,
                exception.getMessage()
        );
        problem.setTitle("Requisição inválida");
        problem.setType(URI.create("urn:rabbit-rescue:bad-request"));
        return ResponseEntity.badRequest().body(problem);
    }
}
