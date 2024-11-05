const Worker = require('./worker.js');

module.exports = class Scheduler {
    constructor(parallelism) {
        this.workers = [];
        for (let i = 0; i < parallelism; i++) {
            this.workers.push(new Worker(`Worker_${i}`));
        }

        this.availaleWorkers = [...this.workers];
        this.customersInQueue = [];
        this.customerQueueOffset = 0;
        this.taskQueueByCustomer = new Map();
        this.runningTaskCountByCustomer = new Map();
    }

    execute(task) {
        const { customerId } =  task;

        const worker = this.lockWorker();
        if (worker !== null) {
            this.executeUsing(worker, task);
            return;
        }

        const customerQueue = this.taskQueueByCustomer.get(customerId);
        if (customerQueue === undefined) {
            this.taskQueueByCustomer.set(customerId, [task]);
            this.customersInQueue.push(customerId);
            return;
        }

        customerQueue.push(task);
    }

    lockWorker() {
        if (this.availaleWorkers.length <= 0) {
            return null;
        }

        const worker = this.availaleWorkers.pop();
        if (worker.isRunningJob) {
            throw new Error('Invalid state');
        }
        return worker;
    }

    releaseWorker(worker) {
        if (worker.isRunningJob) {
            throw new Error('Invalid state');
        }

        this.availaleWorkers.push(worker);
    }

    executeUsing(worker, task) {
        const { customerId } = task;
        this.runningTaskCountByCustomer.set(
            customerId,
            (this.runningTaskCountByCustomer.get(customerId) ?? 0) + 1,
        );
        worker.execute(task).then(
            () => {
                const customerTaskCount = this.runningTaskCountByCustomer.get(customerId);
                if (customerTaskCount === 0) {
                    throw new Error('Customer task counter de-sync');
                } else if (customerTaskCount === 1) {
                    this.runningTaskCountByCustomer.delete(customerId);
                } else {
                    this.runningTaskCountByCustomer.set(customerId, customerTaskCount - 1);
                }
                this.tick(worker);
            },
            console.error
        );
    }

    tick(worker) {
        let nextTask = null;
        try {
            nextTask = this.pickNextTask();
        } catch (e) {
            this.releaseWorker(worker);
            return;
        }

        if (nextTask !== null) {
            this.executeUsing(worker, nextTask);
        } else {
            this.releaseWorker(worker);
        }
    }

    pickNextTask() {
        if (this.customersInQueue.length === 0) return null;

        let nextCustomerId = this.customersInQueue[this.customerQueueOffset];
        let nextCustomerRunningTasks = this.runningTaskCountByCustomer.get(nextCustomerId) ?? 0;

        for (let offset = 1; offset < this.customersInQueue.length; offset += 1) {
            const customerId = this.customersInQueue[
                (this.customerQueueOffset + offset) % this.customersInQueue.length
            ];
            const runningTasks = (this.runningTaskCountByCustomer.get(customerId) ?? 0);

            if (runningTasks < nextCustomerRunningTasks) {
                nextCustomerId = customerId;
                nextCustomerRunningTasks = runningTasks;
            }
        }
        // Comment-out loop above and then uncomment this log to see how unfair first algorithm is
        // It should be less fair for customers with id >= 8000 as they have long-running tasks
        // console.log('nextCustomerRunningTasks', nextCustomerRunningTasks);

        const customerQueue = this.taskQueueByCustomer.get(nextCustomerId);
        if (customerQueue === undefined || customerQueue.length <= 0) {
            throw new Error('Customer queue is empty, but customer is still registered in the offset');
        }

        const customerTask = customerQueue.shift();
        if (customerQueue.length <= 0) {
            this.taskQueueByCustomer.delete(nextCustomerId);
            const offset = this.customersInQueue.findIndex((t) => t === nextCustomerId);
            this.customersInQueue.splice(offset, 1);
        }

        if (this.customersInQueue.length !== 0) {
            this.customerQueueOffset = (this.customerQueueOffset + 1) % this.customersInQueue.length;
        } else {
            this.customerQueueOffset = 0;
        }

        return customerTask;
    }
}