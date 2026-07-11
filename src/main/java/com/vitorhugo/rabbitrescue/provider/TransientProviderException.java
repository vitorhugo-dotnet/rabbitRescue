package com.vitorhugo.rabbitrescue.provider;

public class TransientProviderException extends RuntimeException {

    public TransientProviderException(String message) {
        super(message);
    }
}
