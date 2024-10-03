import { DynamoDBDocumentClient, PutCommand, GetCommand, UpdateCommand, DeleteCommand, QueryCommand, TransactWriteCommand } from "@aws-sdk/lib-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";

/**
 * Configuration options for the DynamoDBSemaphore
 */
interface SemaphoreConfig {
    tableName: string;
    semaphoreName: string;
    maxLocks: number;
    lockTtlSeconds: number;
}

/**
 * A distributed semaphore implementation using DynamoDB
 */
class DynamoDBSemaphore {
    private docClient: DynamoDBDocumentClient;
    private config: SemaphoreConfig;

    constructor(client: DynamoDBClient, config: SemaphoreConfig) {
        this.docClient = DynamoDBDocumentClient.from(client);
        this.config = config;
    }

    /**
     * Attempts to acquire a lock
     * @returns A unique lock ID if successful, null otherwise
     */
    async acquireLock(): Promise<string | null> {
        const now = Math.floor(Date.now() / 1000);
        const lockId = `${this.config.semaphoreName}-${now}-${Math.random().toString(36).substring(2, 15)}`;

        try {
            const result = await this.docClient.send(new TransactWriteCommand({
                TransactItems: [
                    {
                        Update: {
                            TableName: this.config.tableName,
                            Key: { pk: this.config.semaphoreName, sk: 'COUNT' },
                            UpdateExpression: 'SET #count = if_not_exists(#count, :zero) + :inc',
                            ConditionExpression: 'attribute_not_exists(#count) OR #count < :max',
                            ExpressionAttributeNames: { '#count': 'count' },
                            ExpressionAttributeValues: { ':inc': 1, ':max': this.config.maxLocks, ':zero': 0 }
                        }
                    },
                    {
                        Put: {
                            TableName: this.config.tableName,
                            Item: {
                                pk: this.config.semaphoreName,
                                sk: `LOCK#${lockId}`,
                                expiresAt: now + this.config.lockTtlSeconds
                            },
                            ConditionExpression: 'attribute_not_exists(pk)'
                        }
                    }
                ]
            }));

            return lockId;
        } catch (error) {
            console.error('Failed to acquire lock:', error);
            // Cleanup expired locks if we fail to acquire a lock
            await this.cleanupExpiredLocks();
        }

        return null;
    }

    /**
     * Releases a previously acquired lock
     * @param lockId The unique lock ID to release
     */
    async releaseLock(lockId: string): Promise<void> {
        try {
            await this.docClient.send(new TransactWriteCommand({
                TransactItems: [
                    {
                        Delete: {
                            TableName: this.config.tableName,
                            Key: { pk: this.config.semaphoreName, sk: `LOCK#${lockId}` },
                            ConditionExpression: 'attribute_exists(pk)'
                        }
                    },
                    {
                        Update: {
                            TableName: this.config.tableName,
                            Key: { pk: this.config.semaphoreName, sk: 'COUNT' },
                            UpdateExpression: 'SET #count = #count - :dec',
                            ConditionExpression: '#count > :zero',
                            ExpressionAttributeNames: { '#count': 'count' },
                            ExpressionAttributeValues: { ':dec': 1, ':zero': 0 }
                        }
                    }
                ]
            }));
        } catch (error) {
            console.error('Failed to release lock:', error);
        }
    }

    /**
     * Cleans up expired locks for the current semaphore
     */
    private async cleanupExpiredLocks(): Promise<void> {
        const now = Math.floor(Date.now() / 1000);

        try {
            // Query for expired locks
            const queryResult = await this.docClient.send(new QueryCommand({
                TableName: this.config.tableName,
                KeyConditionExpression: 'pk = :pk AND begins_with(sk, :lockPrefix)',
                FilterExpression: 'expiresAt < :now',
                ExpressionAttributeValues: {
                    ':pk': this.config.semaphoreName,
                    ':lockPrefix': 'LOCK#',
                    ':now': now
                }
            }));

            if (!queryResult.Items || queryResult.Items.length === 0) return;

            // Delete expired locks and update count
            const expiredLockCount = queryResult.Items.length;
            for (const item of queryResult.Items) {
                await this.docClient.send(new DeleteCommand({
                    TableName: this.config.tableName,
                    Key: { pk: item.pk, sk: item.sk }
                }));
            }

            // Update the count
            await this.docClient.send(new UpdateCommand({
                TableName: this.config.tableName,
                Key: { pk: this.config.semaphoreName, sk: 'COUNT' },
                UpdateExpression: 'SET #count = #count - :dec',
                ExpressionAttributeNames: { '#count': 'count' },
                ExpressionAttributeValues: { ':dec': expiredLockCount }
            }));

            console.log(`Cleaned up ${expiredLockCount} expired locks for semaphore ${this.config.semaphoreName}`);
        } catch (error) {
            console.error('Failed to cleanup expired locks:', error);
        }
    }
}

// Example usage and tests

async function runTests() {
    const client = new DynamoDBClient({ region: 'us-east-1' });
    const semaphore = new DynamoDBSemaphore(client, {
        tableName: 'MySemaphoreTable',
        semaphoreName: 'TestSemaphore',
        maxLocks: 3,
        lockTtlSeconds: 60
    });

    console.log('Running tests...');

    // Test 1: Acquire locks up to the maximum
    const acquiredLocks: string[] = [];
    for (let i = 0; i < 3; i++) {
        const lockId = await semaphore.acquireLock();
        if (lockId) {
            acquiredLocks.push(lockId);
            console.log(`Acquired lock ${i + 1}: ${lockId}`);
        }
    }

    // Test 2: Try to acquire one more lock (should fail and trigger cleanup)
    const extraLock = await semaphore.acquireLock();
    console.log('Attempting to acquire extra lock:', extraLock ? 'Succeeded (unexpected)' : 'Failed (expected)');

    // Test 3: Release locks
    for (const lockId of acquiredLocks) {
        await semaphore.releaseLock(lockId);
        console.log(`Released lock: ${lockId}`);
    }

    // Test 4: Acquire a lock after releasing
    const newLock = await semaphore.acquireLock();
    console.log('Acquired new lock after releasing:', newLock ? 'Succeeded (expected)' : 'Failed (unexpected)');

    if (newLock) {
        await semaphore.releaseLock(newLock);
    }

    console.log('Tests completed.');
}

// Uncomment the following line to run the tests
if (require.main === module) {
    runTests().catch(error => {
        console.error('Error running tests:', error);
        process.exit(1);
    });
}


export { DynamoDBSemaphore };
