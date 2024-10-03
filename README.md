# Stedi Eligibility Check API Concurrency Management

This repository contains examples demonstrating how to effectively manage concurrency when interacting with Stedi resources, with a particular focus on the Stedi Eligibility Check APIs.

## Overview

Managing concurrency in API interactions, especially with the Stedi Eligibility Check APIs, presents unique challenges that go beyond simple requests per second (RPS) considerations. The complexity arises from the highly variable response times of these APIs, which can range from mere seconds to up to 90 seconds per request.

This variability in response times means that traditional RPS-based concurrency management strategies may not be sufficient. Instead, more sophisticated approaches are needed to ensure efficient use of resources while avoiding overload or rate limiting issues.

## Key Concepts

- **Variable Response Times**: Requests to the Stedi Eligibility Check APIs can take anywhere from a few seconds to 90 seconds to complete.
- **Concurrency vs. RPS**: Due to the wide range of possible response times, concurrency management in this context is not simply about limiting requests per second.
- **Adaptive Strategies**: The examples in this repository demonstrate adaptive concurrency management techniques that account for the unpredictable nature of response times.

## Concurrency Management Strategy

In these examples, we use DynamoDB to implement a semaphore-based concurrency control mechanism. Here's how it works:

1. We attempt to obtain a count on a semaphore up to a configurable limit.
2. If we can't obtain the semaphore, we wait using an exponential backoff strategy with jitter.
3. We continue attempting to obtain the semaphore, but bail after a given amount of time.
4. Once we obtain the semaphore, we perform our API request.
5. After the request is complete (or fails), we release the semaphore.

This approach allows us to manage the concurrency of requests effectively, ensuring we don't overwhelm the Stedi APIs while still maintaining efficient throughput.

### Concurrency Flow Diagram

The following Mermaid diagram illustrates the process of obtaining and releasing semaphores to manage concurrency (up to 3 concurrent requests):

```mermaid
sequenceDiagram
    participant R1 as Request 1
    participant R2 as Request 2
    participant R3 as Request 3
    participant R4 as Request 4
    participant S as Semaphore (Limit: 2)
    participant API as Stedi API

    R1->>S: Attempt to obtain slot
    S->>R1: Slot granted (1/2)
    R1->>API: Make API request

    R2->>S: Attempt to obtain slot
    S->>R2: Slot granted (2/2)
    R2->>API: Make API request

    R3->>S: Attempt to obtain slot
    S-->>R3: No slot available
    Note over R3: Wait (exponential backoff)

    API-->>R1: Response (after 5s)
    R1->>S: Release slot (1/2)

    R3->>S: Retry obtain slot
    S->>R3: Slot granted (2/2)
    R3->>API: Make API request

    R4->>S: Attempt to obtain slot
    S-->>R4: No slot available
    Note over R4: Wait (exponential backoff)
    R4->>S: Retry obtain slot
    S-->>R4: No slot available
    Note over R4: Wait (exponential backoff)
    R4->>S: Retry obtain slot
    Note over R4: Max retries reached
    R4->>R4: Fail (unable to obtain slot)

    API-->>R2: Response (after 30s)
    R2->>S: Release slot (1/2)

    API-->>R3: Response (after 60s)
    R3->>S: Release slot (0/2)
```
