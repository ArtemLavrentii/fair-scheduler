// or you could also do (totalTask = 0; totalTasks < 500; ) and then totalTasks += taskCount inside of loop
for (let batch = 0; batch < 25; batch += 1) {
    const customerId = Math.floor(Math.random() * 10) * 1000;
    // create 5-30 tasks, with higher chance of getting small amount of tasks
    const taskCount = Math.floor(Math.random() * Math.random() * 25) + 5;
    for (let task = 0; task < taskCount; task += 1) {
        await fetch(`http://localhost:3000/queue?customerId=${customerId}&duration=${Math.random() * 2000 + 1000}`);
    }
}