import { DynamoDBDocumentClient, QueryCommand, UpdateCommand, PutCommand, DeleteCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import assert from "assert";

/**
 * Configuration options for the DynamoDBSemaphore
 */
interface SemaphoreConfig {
    tableName: string;
    semaphoreName: string;
    maxLocks: number;
    lockTtlSeconds: number;
    retryConfig?: {
        maxRetries?: number;
        baseDelay?: number;
        maxDelay?: number;
    };
    debug?: boolean; // New option for enabling debug logging
}

/**
 * A distributed semaphore implementation using DynamoDB
 */
class DynamoDBSemaphore {
    private docClient: DynamoDBDocumentClient;
    private config: SemaphoreConfig & {
        retryConfig: Required<NonNullable<SemaphoreConfig['retryConfig']>>
    };
    private isCleaningUp: boolean = false;

    constructor(client: DynamoDBClient, config: SemaphoreConfig) {
        this.docClient = DynamoDBDocumentClient.from(client);
        this.config = {
            ...config,
            retryConfig: {
                maxRetries: config.retryConfig?.maxRetries ?? 30,
                baseDelay: config.retryConfig?.baseDelay ?? 100,
                maxDelay: config.retryConfig?.maxDelay ?? 5000
            }
        };
        if (this.config.debug) {
            console.log(`DynamoDBSemaphore initialized with config:`, JSON.stringify(this.config, null, 2));
        }
    }

    /**
     * Table structure required for the DynamoDBSemaphore:
     * 
     * Table Name: <as specified in SemaphoreConfig.tableName>
     * Primary Key:
     *   - Partition Key (pk): String
     *   - Sort Key (sk): String
     * 
     * Additional Attributes:
     *   - count: Number (used for the semaphore count)
     *   - expiresAt: Number (used for lock expiration)
     * 
     * Example items:
     * 1. Semaphore count item:
     *    { pk: "<semaphoreName>", sk: "COUNT", count: <number> }
     * 
     * 2. Lock items:
     *    { pk: "<semaphoreName>", sk: "LOCK#<lockId>", expiresAt: <timestamp> }
     * 
     * Note: Ensure that the table has the capacity to handle the expected read and write throughput.
     */

    private async retryWithBackoff<T>(operation: () => Promise<T>): Promise<T> {
        let retryCount = 0;
        const { maxRetries, baseDelay, maxDelay } = this.config.retryConfig;
        while (true) {
            try {
                return await operation();
            } catch (error) {
                if (retryCount >= maxRetries) {
                    if (this.config.debug) {
                        console.error(`Max retries (${maxRetries}) reached. Throwing error:`, error);
                    }
                    throw error;
                }
                const exponentialDelay = Math.min(maxDelay, baseDelay * Math.pow(2, retryCount));
                const jitter = Math.random() * exponentialDelay * 0.1; // 10% jitter
                const delay = exponentialDelay + jitter;
                if (this.config.debug) {
                    console.log(`Retry ${retryCount + 1}/${maxRetries}. Waiting for ${delay.toFixed(2)}ms before next attempt.`);
                }
                await new Promise(resolve => setTimeout(resolve, delay));
                retryCount++;
            }
        }
    }

    /**
     * Attempts to acquire a lock
     * @returns A unique lock ID if successful, null otherwise
     */
    async acquireLock(): Promise<string | null> {
        if (this.config.debug) {
            console.log(`Attempting to acquire lock for semaphore: ${this.config.semaphoreName}`);
        }
        return this.retryWithBackoff(async () => {
            const now = Math.floor(Date.now() / 1000);
            const lockId = `${this.config.semaphoreName}-${now}-${Math.random().toString(36).substring(2, 15)}`;

            try {
                // First, try to increment the count
                await this.docClient.send(new UpdateCommand({
                    TableName: this.config.tableName,
                    Key: { pk: this.config.semaphoreName, sk: 'COUNT' },
                    UpdateExpression: 'SET #count = if_not_exists(#count, :zero) + :inc',
                    ConditionExpression: '#count < :max',
                    ExpressionAttributeNames: { '#count': 'count' },
                    ExpressionAttributeValues: { ':inc': 1, ':max': this.config.maxLocks, ':zero': 0 }
                }));

                if (this.config.debug) {
                    console.log(`Successfully incremented count for semaphore: ${this.config.semaphoreName}`);
                }

                // If successful, try to create the lock
                await this.docClient.send(new PutCommand({
                    TableName: this.config.tableName,
                    Item: {
                        pk: this.config.semaphoreName,
                        sk: `LOCK#${lockId}`,
                        expiresAt: now + this.config.lockTtlSeconds
                    },
                    ConditionExpression: 'attribute_not_exists(pk)'
                }));

                if (this.config.debug) {
                    console.log(`Successfully acquired lock: ${lockId}`);
                }

                return lockId;
            } catch (error) {
                if (this.config.debug) {
                    console.error(`Failed to acquire lock:`, error);
                    console.log(`Attempting to cleanup expired locks...`);
                }
                // Cleanup expired locks if we fail to acquire a lock
                await this.cleanupExpiredLocks();
                return null;
            }
        });
    }

    /**
     * Releases a previously acquired lock
     * @param lockId The unique lock ID to release
     * @returns A boolean indicating whether the lock was successfully released
     */
    async releaseLock(lockId: string): Promise<boolean> {
        if (this.config.debug) {
            console.log(`Attempting to release lock: ${lockId}`);
        }
        // there are cases where the lock record gets removed but we couldn't decrement the count
        // (for example, throttling on the table at high volume)
        // we try our best to clean up after ourselves and release the lock if possible
        let deleted = false;

        // handle retries easily for us with retryWithBackoff
        return this.retryWithBackoff(async () => {
            if (!deleted) {
                try {
                    const deleteResult = await this.docClient.send(new DeleteCommand({
                        TableName: this.config.tableName,
                        Key: { pk: this.config.semaphoreName, sk: `LOCK#${lockId}` },
                        ConditionExpression: 'attribute_exists(pk)'
                    }));
                    if (deleteResult.$metadata.httpStatusCode === 200) {
                        deleted = true;
                        if (this.config.debug) {
                            console.log(`Successfully deleted lock record: ${lockId}`);
                        }
                    }
                    else {
                        if (this.config.debug) {
                            console.error(`Failed to delete lock record: ${lockId}`);
                        }
                        throw new Error('Failed to release lock');
                    }
                } catch (error: any) {
                    if (error.name === 'ConditionalCheckFailedException') {
                        if (this.config.debug) {
                            console.log(`Lock record not found, assuming already released: ${lockId}`);
                        }
                        return true;
                    }
                }
            }

            // If successful, decrement the count
            try {
                await this.docClient.send(new UpdateCommand({
                    TableName: this.config.tableName,
                    Key: { pk: this.config.semaphoreName, sk: 'COUNT' },
                    UpdateExpression: 'SET #count = if_not_exists(#count, :zero) - :dec',
                    ConditionExpression: '#count > :zero',
                    ExpressionAttributeNames: { '#count': 'count' },
                    ExpressionAttributeValues: { ':dec': 1, ':zero': 0 }
                }));
            } catch (error: any) {
                if (error.name === 'ConditionalCheckFailedException') {
                    // If condition check failed, we consider it successful
                    if (this.config.debug) {
                        console.log(`Condition check failed when decrementing count, assuming successful release for lock: ${lockId}`);
                    }
                } else {
                    // For other errors, we still throw
                    throw error;
                }
            }

            if (this.config.debug) {
                console.log(`Successfully released lock: ${lockId}`);
            }

            return true;
        });
    }

    /**
     * Cleans up expired locks for the current semaphore
     */
    async cleanupExpiredLocks(): Promise<void> {
        if (this.isCleaningUp) {
            if (this.config.debug) {
                console.log(`Cleanup already in progress for semaphore: ${this.config.semaphoreName}`);
            }
            return;
        }

        this.isCleaningUp = true;

        if (this.config.debug) {
            console.log(`Starting cleanup of expired locks for semaphore: ${this.config.semaphoreName}`);
        }

        try {
            await this.retryWithBackoff(async () => {
                const now = Math.floor(Date.now() / 1000);

                try {
                    // Query for all locks (expired and non-expired)
                    const queryResult = await this.docClient.send(new QueryCommand({
                        TableName: this.config.tableName,
                        KeyConditionExpression: 'pk = :pk AND begins_with(sk, :lockPrefix)',
                        ExpressionAttributeValues: {
                            ':pk': this.config.semaphoreName,
                            ':lockPrefix': 'LOCK#'
                        }
                    }));

                    const allLocks = queryResult.Items || [];
                    const expiredLocks = allLocks.filter(item => item.expiresAt < now);

                    if (this.config.debug) {
                        console.log(`Found ${allLocks.length} total locks, ${expiredLocks.length} expired`);
                    }

                    // Get the current count
                    const countResult = await this.docClient.send(new QueryCommand({
                        TableName: this.config.tableName,
                        KeyConditionExpression: 'pk = :pk AND sk = :sk',
                        ExpressionAttributeValues: {
                            ':pk': this.config.semaphoreName,
                            ':sk': 'COUNT'
                        }
                    }));

                    const currentCount = countResult.Items && countResult.Items[0] ? countResult.Items[0].count : 0;
                    const actualLockCount = allLocks.length;

                    if (this.config.debug) {
                        console.log(`Current count: ${currentCount}, Actual lock count: ${actualLockCount}`);
                    }

                    // Release expired locks
                    for (const item of expiredLocks) {
                        const lockId = item.sk.split('#')[1];
                        await this.releaseLock(lockId);
                        if (this.config.debug) {
                            console.log(`Released expired lock: ${lockId}`);
                        }
                    }

                    // it is still possible to totally deadlock, if when records are removed we never decrement the count
                    // due to network issues or throttling. This is rare, but possible.
                    // there is no _great_ way to handle this so possibly alarm in your own systems if throughput drops lower than expected
                } catch (error) {
                    if (this.config.debug) {
                        console.error(`Error during cleanup:`, error);
                    }
                    throw error; // Rethrow to trigger retry
                }
            });
        } finally {
            this.isCleaningUp = false;
        }
    }
}

// Example usage and tests

/**
 * Test 1: Acquire locks up to the maximum
 * This test attempts to acquire the maximum number of locks allowed by the semaphore.
 * It verifies that we can acquire all locks successfully.
 */
async function testAcquireMaxLocks(semaphore: DynamoDBSemaphore): Promise<string[]> {
    console.log('Test 1: Acquire locks up to the maximum');
    const acquiredLocks: string[] = [];
    for (let i = 0; i < 3; i++) {
        const lockId = await semaphore.acquireLock();
        assert(lockId !== null, `Failed to acquire lock ${i + 1}`);
        acquiredLocks.push(lockId!);
        console.log(`Acquired lock ${i + 1}: ${lockId}`);
    }
    assert(acquiredLocks.length === 3, 'Failed to acquire all 3 locks');
    return acquiredLocks;
}

/**
 * Test 2: Try to acquire one more lock beyond the maximum
 * This test attempts to acquire an additional lock beyond the maximum allowed.
 * It verifies that the semaphore correctly prevents acquiring more than the maximum locks.
 */
async function testAcquireExtraLock(semaphore: DynamoDBSemaphore) {
    console.log('Test 2: Try to acquire one more lock (should fail and trigger cleanup)');
    const extraLock = await semaphore.acquireLock();
    assert(extraLock === null, 'Unexpectedly acquired an extra lock');
    console.log('Attempting to acquire extra lock: Failed (as expected)');
}

/**
 * Test 3: Release acquired locks
 * This test releases all the locks acquired in Test 1.
 * It verifies that we can successfully release all acquired locks.
 */
async function testReleaseLocks(semaphore: DynamoDBSemaphore, acquiredLocks: string[]) {
    console.log('Test 3: Release locks');
    for (const lockId of acquiredLocks) {
        const released = await semaphore.releaseLock(lockId);
        assert(released, `Failed to release lock: ${lockId}`);
        console.log(`Released lock: ${lockId}`);
    }
}

/**
 * Test 4: Acquire a lock after releasing
 * This test attempts to acquire a new lock after releasing all previous locks.
 * It verifies that the semaphore allows acquiring a new lock after releasing.
 */
async function testAcquireAfterRelease(semaphore: DynamoDBSemaphore) {
    console.log('Test 4: Acquire a lock after releasing');
    const newLock = await semaphore.acquireLock();
    assert(newLock !== null, 'Failed to acquire new lock after releasing');
    console.log('Acquired new lock after releasing:', newLock);

    if (newLock) {
        const released = await semaphore.releaseLock(newLock);
        assert(released, 'Failed to release the new lock');
    }
}

/**
 * Test 5: Throughput test at concurrency of 15
 * This test simulates high concurrency usage of the semaphore.
 * It attempts to perform a large number of lock acquisitions and releases concurrently.
 */
async function testThroughput(client: DynamoDBClient) {
    console.log('Test 5: Throughput test at concurrency of 15');
    const concurrency = 100;
    const totalOperations = 1000;
    const semaphoreForThroughput = new DynamoDBSemaphore(client, {
        tableName: 'MySemaphoreTable',
        semaphoreName: 'ThroughputTestSemaphore',
        maxLocks: concurrency,
        lockTtlSeconds: 60,
        retryConfig: {
            maxRetries: 30,
            baseDelay: 500,
            maxDelay: 5_000
        },
        debug: true
    });

    const startTime = Date.now();

    const operations = Array(totalOperations).fill(null).map(async (_, index) => {
        let lockId = null;
        while (!lockId) {
            lockId = await semaphoreForThroughput.acquireLock();
            if (!lockId) {
                await new Promise(resolve => setTimeout(resolve, 100));
            } else {
                console.log(`Acquired lock ${lockId}`);
            }
        }
        // Simulate some work
        await new Promise(resolve => setTimeout(resolve, 1500));
        await semaphoreForThroughput.releaseLock(lockId);
        return true;
    });

    const results = await Promise.all(operations);
    const endTime = Date.now();

    const successfulOperations = results.filter(Boolean).length;
    const totalTime = (endTime - startTime) / 1000; // in seconds
    const throughput = successfulOperations / totalTime;

    console.log(`Throughput test results:`);
    console.log(`Total operations: ${totalOperations}`);
    console.log(`Successful operations: ${successfulOperations}`);
    console.log(`Total time: ${totalTime.toFixed(2)} seconds`);
    console.log(`Throughput: ${throughput.toFixed(2)} operations per second`);

    assert(successfulOperations > 0, 'No successful operations in throughput test');
    assert(throughput > 0, 'Throughput is zero or negative');

    console.log('Throughput test completed successfully.');
}

/**
 * Test 6: Cleanup expired locks
 * This test verifies the semaphore's ability to clean up expired locks.
 * It creates locks with a very short TTL, waits for them to expire, and then verifies cleanup.
 */
async function testCleanupExpiredLocks(client: DynamoDBClient) {
    console.log('Test 6: Cleanup expired locks');
    const cleanupSemaphore = new DynamoDBSemaphore(client, {
        tableName: 'MySemaphoreTable',
        semaphoreName: 'CleanupTestSemaphore',
        maxLocks: 5,
        lockTtlSeconds: 1, // Set a very short TTL for testing
        debug: true
    });

    // Acquire 5 locks
    const cleanupLocks: string[] = [];
    for (let i = 0; i < 5; i++) {
        const lockId = await cleanupSemaphore.acquireLock();
        assert(lockId !== null, `Failed to acquire cleanup test lock ${i + 1}`);
        cleanupLocks.push(lockId!);
    }

    // Wait for locks to expire
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Try to acquire a new lock, which should trigger cleanup
    const cleanupNewLock = await cleanupSemaphore.acquireLock();
    assert(cleanupNewLock === null, 'should fail but trigger cleanup');

    // Verify that we can acquire all locks again
    for (let i = 0; i < 4; i++) {
        const lockId = await cleanupSemaphore.acquireLock();
        assert(lockId !== null, `Failed to acquire lock ${i + 1} after cleanup`);
        await cleanupSemaphore.releaseLock(lockId!);
    }

    console.log('Cleanup expired locks test completed successfully.');
}

async function runTests() {
    const client = new DynamoDBClient({ region: 'us-east-1' });
    const semaphore = new DynamoDBSemaphore(client, {
        tableName: 'MySemaphoreTable',
        semaphoreName: 'TestSemaphore',
        maxLocks: 3,
        lockTtlSeconds: 5,
        retryConfig: {
            maxRetries: 1,
            baseDelay: 500,
            maxDelay: 10000
        },
        debug: true // Enable debug logging for tests
    });
    await semaphore.cleanupExpiredLocks();

    console.log('Running tests...');

    const acquiredLocks = await testAcquireMaxLocks(semaphore);
    await testAcquireExtraLock(semaphore);
    await testReleaseLocks(semaphore, acquiredLocks);
    await testAcquireAfterRelease(semaphore);
    await testThroughput(client);
    await testCleanupExpiredLocks(client);

    console.log('All tests passed successfully.');
}

// Uncomment the following line to run the tests
if (require.main === module) {
    runTests().catch(error => {
        console.error('Error running tests:', error);
        process.exit(1);
    });
}

export { DynamoDBSemaphore };
