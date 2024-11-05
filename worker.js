module.exports = class Worker {
    constructor(name) {
        this.name = name;
        this.currentTask = null;
    }

    get isRunningJob() {
        return this.currentTask !== null;
    }

    async execute(currentTask) {
        if (this.currentTask !== null) {
            throw new Error('Worker is busy');
        }

        this.currentTask = currentTask;
        try {
            await new Promise((resolve) => setTimeout(resolve, currentTask.duration));
            console.log('worker', this.name, 'finished task', currentTask);
        } finally {
            this.currentTask = null;
        }
    }
}