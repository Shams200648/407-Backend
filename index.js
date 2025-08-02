import express from "express";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";
import { fetchDeviceStatus, controlDeviceSwitch } from "./tuya.js";
import { WebSocketServer } from "ws";
import http from "http";
import cors from "cors";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json()); // Add this to parse JSON request bodies
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const PORT = process.env.PORT || 5000;
const client = new MongoClient(process.env.MONGO_URI);
const dbName = "tuya";
const collectionName = "device_data";

await client.connect();
console.log("Connected to MongoDB Atlas");
const db = client.db(dbName);
const collection = db.collection(collectionName);

const deviceId = process.env.TUYA_DEVICE_ID;

function broadcast(data) {
  const json = JSON.stringify(data);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(json);
  });
}

// Polling configuration
let pollingInterval = 2000; // Start with 2 seconds
let consecutiveFailures = 0;
const maxFailures = 5;
const maxInterval = 30000; // Max 30 seconds between polls
const maxConsecutiveFailures = 20; // Restart server after 20 consecutive failures
let lastSuccessfulPoll = Date.now();

// Improved polling function with retry logic and auto-restart
async function pollDeviceStatus() {
  try {
    const status = await fetchDeviceStatus(deviceId);

    // Reset on successful poll
    consecutiveFailures = 0;
    pollingInterval = 2000; // Reset to normal interval
    lastSuccessfulPoll = Date.now();

    const doc = {
      timestamp: new Date(),
      status,
    };
    await collection.insertOne(doc);

    const transformed = {
      time: doc.timestamp.toISOString(),
      current: getValue(status, "cur_current"),
      voltage: getValue(status, "cur_voltage"),
      power: getValue(status, "cur_power"),
    };

    broadcast(transformed);
    console.log(`Polling successful at ${new Date().toISOString()}`);

  } catch (err) {
    consecutiveFailures++;
    console.error(`Polling failed (attempt ${consecutiveFailures}):`, err.message);

    // Check if we should restart the server
    if (consecutiveFailures >= maxConsecutiveFailures) {
      console.error(`CRITICAL: ${consecutiveFailures} consecutive failures. Restarting server in 10 seconds...`);

      // Send a final error message to connected clients
      broadcast({
        error: "Server restarting due to API issues",
        timestamp: new Date().toISOString()
      });

      // Wait 10 seconds then restart
      setTimeout(() => {
        console.error(`RESTARTING SERVER due to persistent API failures`);
        process.exit(1); // Exit with error code to trigger restart
      }, 20000);

      return; // Stop polling
    }

    // Implement exponential backoff
    if (consecutiveFailures >= maxFailures) {
      pollingInterval = Math.min(pollingInterval * 2, maxInterval);
      console.log(`Increasing polling interval to ${pollingInterval}ms due to ${consecutiveFailures} consecutive failures`);
    }

    // Don't broadcast on failure to avoid sending stale data
  }
}

// Start polling with the improved function
setInterval(pollDeviceStatus, 2000);

app.get("/data", async (req, res) => {
  // const result = await collection
  //   .find({})
  //   .sort({ timestamp: -1 })
  //   .limit(60)
  //   .toArray();
  //
  // res.json(
  //   result.map((entry) => ({
  //     time: entry.timestamp.toLocaleTimeString(),
  //     ...Object.fromEntries(entry.status.map((s) => [s.code, s.value])),
  //   })),
  // );
  res.json("msg :hello");
});

// Switch control endpoint
app.post("/switch", async (req, res) => {
  try {
    const { state } = req.body; // state should be true for on, false for off

    if (typeof state !== 'boolean') {
      return res.status(400).json({
        success: false,
        error: "Invalid state parameter. Must be true (on) or false (off)"
      });
    }

    const result = await controlDeviceSwitch(deviceId, state);

    console.log("Tuya API response:", JSON.stringify(result, null, 2));

    if (result && result.success !== false) {
      res.json({
        success: true,
        message: `Device switched ${state ? 'on' : 'off'} successfully`,
        data: result
      });
    } else {
      res.status(500).json({
        success: false,
        error: "Failed to control device switch",
        data: result
      });
    }

  } catch (error) {
    console.error("Error controlling device switch:", error);
    res.status(500).json({
      success: false,
      error: "Failed to control device switch",
      details: error.message
    });
  }
});

// Get current switch status endpoint
app.get("/switch-status", async (req, res) => {
  try {
    const status = await fetchDeviceStatus(deviceId);

    if (!status || !Array.isArray(status)) {
      return res.status(500).json({
        success: false,
        error: "Invalid response from Tuya API"
      });
    }

    // Find the switch_1 status
    const switchStatus = status.find(s => s.code === "switch_1");

    if (!switchStatus) {
      return res.status(404).json({
        success: false,
        error: "Switch status not found in device data"
      });
    }

    res.json({
      success: true,
      data: {
        switch: switchStatus.value, // true for on, false for off
        timestamp: new Date().toISOString(),
        lastUpdated: new Date().toISOString()
      }
    });

  } catch (error) {
    console.error("Error fetching switch status:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch switch status from Tuya API",
      details: error.message
    });
  }
});


async function getTodayDataFromDB(timezone = 'Asia/Dhaka') {
  console.log("--- Running Optimized MongoDB Aggregation for Today's Data ---");
  console.log(`Using timezone: ${timezone}`);

  const todayStart = getTodayStartInTimezone(timezone);
  const todayEnd = getTodayEndInTimezone(timezone);

  console.log(`Today start (${timezone}): ${todayStart.toISOString()}`);
  console.log(`Today end (${timezone}): ${todayEnd.toISOString()}`);
  console.log(`Query range: ${todayStart.toISOString()} to ${todayEnd.toISOString()}`);

  const pipeline = [
    {
      $match: {
        timestamp: { $gte: todayStart, $lte: todayEnd }
      }
    },
    {
      $unwind: "$status"
    },
    {
      $group: {
        _id: {
          hour: {
            $hour: {
              date: "$timestamp",
              timezone: "Asia/Dhaka"
            }
          },
          code: "$status.code"
        },
        value: { $avg: "$status.value" }
      }
    },
    {
      $group: {
        _id: "$_id.hour",
        power: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_power"] },
              { $divide: ["$value", 10] },
              null
            ]
          }
        },
        current: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_current"] },
              "$value",
              null
            ]
          }
        },
        voltage: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_voltage"] },
              { $divide: ["$value", 10] },
              null
            ]
          }
        }
      }
    },
    {
      $sort: { _id: 1 }
    }
  ];

  const result = await collection.aggregate(pipeline).toArray();

  // Debug: Log the actual hours found
  const hoursFound = result.map(h => h._id).sort((a, b) => a - b);
  console.log(`Hours with data: ${hoursFound.join(', ')}`);

  // Create the 24-hour structure
  const todayData = createEmptyTodayData();
  result.forEach(hourData => {
    const hour = hourData._id;
    if (hourData.power !== null) todayData[hour].power = hourData.power;
    if (hourData.current !== null) todayData[hour].current = hourData.current;
    if (hourData.voltage !== null) todayData[hour].voltage = hourData.voltage;
  });

  console.log(`Today aggregation finished. Found data for ${result.length} hours.`);
  return todayData;
}

async function getWeekDataFromDB(timezone = 'Asia/Dhaka') {
  console.log("--- Running Optimized MongoDB Aggregation for Week Data ---");
  console.log(`Using timezone: ${timezone}`);

  const sevenDaysAgo = new Date();
  sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 6);
  let localSevenDaysAgo;
  if (timezone === 'Asia/Dhaka') {
    localSevenDaysAgo = new Date(sevenDaysAgo.getTime() + (6 * 60 * 60 * 1000));
    localSevenDaysAgo.setUTCHours(0, 0, 0, 0);
  } else {
    localSevenDaysAgo = new Date(sevenDaysAgo);
    localSevenDaysAgo.setHours(0, 0, 0, 0);
  }

  const pipeline = [
    {
      $match: {
        timestamp: { $gte: localSevenDaysAgo }
      }
    },
    {
      $unwind: "$status"
    },
    {
      $group: {
        _id: {
          date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp", timezone: timezone } },
          code: "$status.code"
        },
        value: { $avg: "$status.value" }
      }
    },
    {
      $group: {
        _id: "$_id.date",
        power: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_power"] },
              { $divide: ["$value", 10] },
              null
            ]
          }
        },
        current: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_current"] },
              "$value",
              null
            ]
          }
        },
        voltage: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_voltage"] },
              { $divide: ["$value", 10] },
              null
            ]
          }
        }
      }
    },
    {
      $sort: { _id: 1 }
    }
  ];

  const result = await collection.aggregate(pipeline).toArray();

  // Create the 7-day structure
  const week = createEmptyWeekData(timezone);
  result.forEach(dayData => {
    const dayIndex = week.findIndex(d => d.date === dayData._id);
    if (dayIndex !== -1) {
      if (dayData.power !== null) week[dayIndex].power = dayData.power;
      if (dayData.current !== null) week[dayIndex].current = dayData.current;
      if (dayData.voltage !== null) week[dayIndex].voltage = dayData.voltage;
    }
  });

  console.log(`Week aggregation finished. Found data for ${result.length} days.`);
  return week;
}

async function getMonthlyDataFromDB(timezone = 'Asia/Dhaka') {
  console.log("--- Running Optimized MongoDB Aggregation for Monthly Data ---");
  console.log(`Using timezone: ${timezone}`);

  const thirtyDaysAgo = new Date();
  thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
  let localThirtyDaysAgo;
  if (timezone === 'Asia/Dhaka') {
    localThirtyDaysAgo = new Date(thirtyDaysAgo.getTime() + (6 * 60 * 60 * 1000));
  } else {
    localThirtyDaysAgo = new Date(thirtyDaysAgo);
  }

  const pipeline = [
    {
      $match: {
        timestamp: { $gte: localThirtyDaysAgo }
      }
    },
    {
      $unwind: "$status"
    },
    {
      $group: {
        _id: {
          date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp", timezone: timezone } },
          code: "$status.code"
        },
        value: { $avg: "$status.value" }
      }
    },
    {
      $group: {
        _id: "$_id.date",
        power: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_power"] },
              { $divide: ["$value", 10] },
              null
            ]
          }
        },
        current: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_current"] },
              "$value",
              null
            ]
          }
        },
        voltage: {
          $avg: {
            $cond: [
              { $eq: ["$_id.code", "cur_voltage"] },
              { $divide: ["$value", 10] },
              null
            ]
          }
        }
      }
    },
    {
      $sort: { _id: 1 }
    }
  ];

  const result = await collection.aggregate(pipeline).toArray();

  // Create the 30-day structure
  const month = createEmptyMonthData(timezone);
  result.forEach(dayData => {
    const dayIndex = month.findIndex(d => d.date === dayData._id);
    if (dayIndex !== -1) {
      if (dayData.power !== null) month[dayIndex].power = dayData.power;
      if (dayData.current !== null) month[dayIndex].current = dayData.current;
      if (dayData.voltage !== null) month[dayIndex].voltage = dayData.voltage;
    }
  });

  console.log(`Month aggregation finished. Found data for ${result.length} days.`);
  return month;
}

// Simple helper functions
function getValue(statusArray, code) {
  if (!statusArray || !Array.isArray(statusArray)) return 0;
  const item = statusArray.find((s) => s.code === code);
  if (!item) return 0;
  if (code === "cur_voltage") return item.value / 10;
  if (code === "cur_power") return item.value / 10;
  if (code === "cur_current") return item.value;
  return item.value;
}

function getLocalDateString(date, timezone = 'Asia/Dhaka') {
  if (timezone === 'Asia/Dhaka') {
    const dhakaTime = new Date(date.getTime() + (6 * 60 * 60 * 1000));
    return dhakaTime.toISOString().split('T')[0]; // YYYY-MM-DD format
  }
  return date.toLocaleDateString('en-CA');
}

function getLocalHour(date, timezone = 'Asia/Dhaka') {
  if (timezone === 'Asia/Dhaka') {
    const dhakaTime = new Date(date.getTime() + (6 * 60 * 60 * 1000));
    return dhakaTime.getUTCHours();
  }
  return date.getHours();
}

function createEmptyTodayData() {
  const today = [];
  for (let hour = 0; hour < 24; hour++) {
    today.push({ hour, power: 0, current: 0, voltage: 0 });
  }
  return today;
}

function createEmptyWeekData(timezone = 'Asia/Dhaka') {
  const week = [];
  const today = new Date();
  console.log("Creating week data for today:", getLocalDateString(today, timezone));

  for (let i = 6; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(date.getDate() - i);
    const dateString = getLocalDateString(date, timezone);
    console.log(`Week day ${i}: ${dateString}`);
    week.push({
      date: dateString,
      power: 0,
      current: 0,
      voltage: 0
    });
  }
  return week;
}

function createEmptyMonthData(timezone = 'Asia/Dhaka') {
  const month = [];
  const today = new Date();
  console.log("Creating month data for today:", getLocalDateString(today, timezone));

  for (let i = 29; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(date.getDate() - i);
    const dateString = getLocalDateString(date, timezone);
    if (i % 5 === 0) console.log(`Month day ${i}: ${dateString}`);
    month.push({
      date: dateString,
      power: 0,
      current: 0,
      voltage: 0
    });
  }
  return month;
}

app.get("/main-chart/data", async (req, res) => {
  try {
    console.log("=== OPTIMIZED CHART DATA REQUEST ===");

    const timezone = getUserTimezone(req);
    console.log(`Request timezone: ${timezone}`);

    // Run all aggregations in parallel for maximum efficiency
    const [todayData, weekData, monthData] = await Promise.all([
      getTodayDataFromDB(timezone),
      getWeekDataFromDB(timezone),
      getMonthlyDataFromDB(timezone)
    ]);

    res.json({
      success: true,
      data: {
        today: todayData,
        week: weekData,
        month: monthData
      }
    });

  } catch (error) {
    console.error("Error fetching chart data:", error);
    res.status(500).json({
      success: false,
      error: "Failed to fetch chart data",
      details: error.message
    });
  }
});

// Health check endpoint
app.get("/health", (req, res) => {
  const uptime = process.uptime();
  const memoryUsage = process.memoryUsage();

  res.json({
    status: "healthy",
    uptime: `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m ${Math.floor(uptime % 60)}s`,
    memory: {
      rss: `${Math.round(memoryUsage.rss / 1024 / 1024)}MB`,
      heapUsed: `${Math.round(memoryUsage.heapUsed / 1024 / 1024)}MB`,
      heapTotal: `${Math.round(memoryUsage.heapTotal / 1024 / 1024)}MB`
    },
    polling: {
      consecutiveFailures,
      lastSuccessfulPoll: new Date(lastSuccessfulPoll).toISOString(),
      pollingInterval
    },
    timestamp: new Date().toISOString()
  });
});

// Timezone test endpoint
app.get("/timezone-test", (req, res) => {
  const timezone = getUserTimezone(req);
  const now = new Date();

  res.json({
    serverTime: now.toISOString(),
    serverTimezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    requestedTimezone: timezone,
    localTime: getDateInTimezone(now, timezone).toISOString(),
    todayStart: getTodayStartInTimezone(timezone).toISOString(),
    todayEnd: getTodayEndInTimezone(timezone).toISOString()
  });
});

// Database debug endpoint
app.get("/debug-data", async (req, res) => {
  try {
    const timezone = getUserTimezone(req);
    const todayStart = getTodayStartInTimezone(timezone);
    const todayEnd = getTodayEndInTimezone(timezone);

    // Get latest 10 records
    const latestRecords = await collection
      .find({})
      .sort({ timestamp: -1 })
      .limit(10)
      .toArray();

    // Get today's record count
    const todayCount = await collection.countDocuments({
      timestamp: { $gte: todayStart, $lte: todayEnd }
    });

    // Get total record count
    const totalCount = await collection.countDocuments({});

    res.json({
      timezone,
      todayStart: todayStart.toISOString(),
      todayEnd: todayEnd.toISOString(),
      todayRecordCount: todayCount,
      totalRecordCount: totalCount,
      latestRecords: latestRecords.map(r => ({
        timestamp: r.timestamp.toISOString(),
        status: r.status
      }))
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Manual restart endpoint (for emergencies)
app.post("/restart", (req, res) => {
  console.log("Manual restart requested");
  res.json({
    message: "Server restarting in 5 seconds...",
    timestamp: new Date().toISOString()
  });

  setTimeout(() => {
    console.log("MANUAL RESTART triggered");
    process.exit(0);
  }, 5000);
});

// Timezone handling
function getUserTimezone(req) {
  // Try to get timezone from request headers or query params, default to GMT+6
  const timezone = req.query.timezone || req.headers['x-timezone'] || 'Asia/Dhaka';
  return timezone;
}

function getTodayStartInTimezone(timezone) {
  const now = new Date();
  // For Asia/Dhaka (GMT+6), we need to find the UTC time that corresponds to 00:00:00 Dhaka time
  if (timezone === 'Asia/Dhaka') {
    // Get current date in Dhaka timezone
    const dhakaDate = new Date(now.getTime() + (6 * 60 * 60 * 1000));
    // Set to start of day in Dhaka timezone (00:00:00)
    dhakaDate.setUTCHours(0, 0, 0, 0);
    // Convert back to UTC (subtract 6 hours)
    const utcStart = new Date(dhakaDate.getTime() - (6 * 60 * 60 * 1000));
    return utcStart;
  }
  // Default UTC behavior
  const localDate = new Date(now);
  localDate.setHours(0, 0, 0, 0);
  return localDate;
}

function getTodayEndInTimezone(timezone) {
  const now = new Date();
  // For Asia/Dhaka (GMT+6), we need to find the UTC time that corresponds to 23:59:59 Dhaka time
  if (timezone === 'Asia/Dhaka') {
    // Get current date in Dhaka timezone
    const dhakaDate = new Date(now.getTime() + (6 * 60 * 60 * 1000));
    // Set to end of day in Dhaka timezone (23:59:59)
    dhakaDate.setUTCHours(23, 59, 59, 999);
    // Convert back to UTC (subtract 6 hours)
    const utcEnd = new Date(dhakaDate.getTime() - (6 * 60 * 60 * 1000));
    return utcEnd;
  }
  // Default UTC behavior
  const localDate = new Date(now);
  localDate.setHours(23, 59, 59, 999);
  return localDate;
}

server.listen(PORT, () => {
  console.log(`Server running (HTTP + WebSocket) on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
  console.log(`Timezone test: http://localhost:${PORT}/timezone-test`);
  console.log(`Debug data: http://localhost:${PORT}/debug-data`);
  console.log(`Manual restart: POST http://localhost:${PORT}/restart`);
});
