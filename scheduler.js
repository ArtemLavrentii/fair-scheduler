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
        worker.execute(task).then(
            () => {
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

        const nextQueueOffset = this.customerQueueOffset;
        const customerId = this.customersInQueue[nextQueueOffset];

        const customerQueue = this.taskQueueByCustomer.get(customerId);
        if (customerQueue === undefined || customerQueue.length <= 0) {
            throw new Error('Customer queue is empty, but customer is still registered in the offset');
        }

        const customerTask = customerQueue.shift();
        if (customerQueue.length <= 0) {
            this.taskQueueByCustomer.delete(customerId);
            this.customersInQueue.splice(nextQueueOffset, 1);
        }

        if (this.customersInQueue.length !== 0) {
            this.customerQueueOffset = (this.customerQueueOffset + 1) % this.customersInQueue.length;
        } else {
            this.customerQueueOffset = 0;
        }

        return customerTask;
    }
}