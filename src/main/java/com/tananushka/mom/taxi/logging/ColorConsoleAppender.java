package com.tananushka.mom.taxi.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import org.slf4j.MDC;

public class ColorConsoleAppender extends ConsoleAppender<ILoggingEvent> {

    @Override
    protected void append(ILoggingEvent event) {
        String vehicleId = MDC.get("vehicleId");
        if ("taxi1".equals(vehicleId)) {
            System.out.print("\033[31m"); // Red
        } else if ("taxi2".equals(vehicleId)) {
            System.out.print("\033[32m"); // Green
        } else if ("taxi3".equals(vehicleId)) {
            System.out.print("\033[34m"); // Blue
        } else if ("taxi4".equals(vehicleId)) {
            System.out.print("\033[33m"); // Yellow
        } else {
            System.out.print("\033[0m"); // Reset to default color
        }
        super.append(event);
        System.out.print("\033[0m");
    }
}
