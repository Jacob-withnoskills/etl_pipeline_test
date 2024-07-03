@smoke
Feature: Data Factory Smoke Tests

Scenario: Successful Pipeline Execution and Status Check (Scheduled Job)
    Given an Azure Data Factory scheduled pipeline named "pipelineTest"
    When the scheduled pipeline is triggered
    Then the pipeline status should be "Succeeded" within 4 hours
    