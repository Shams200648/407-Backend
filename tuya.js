import crypto from "crypto";
import axios from "axios";
import dotenv from "dotenv";

dotenv.config();

const { TUYA_CLIENT_ID, TUYA_CLIENT_SECRET, TUYA_API_REGION } = process.env;

const BASE_URL = `https://openapi.${TUYA_API_REGION}.com`;

let cachedToken = null;
let cachedTokenExpire = 0;

function genSignature({ method, url, body, t, accessToken = "" }) {
  const contentHash = crypto
    .createHash("sha256")
    .update(body || "")
    .digest("hex");
  const stringToSign = [method, contentHash, "", url].join("\n");
  const signStr = TUYA_CLIENT_ID + accessToken + t + stringToSign;
  return crypto
    .createHmac("sha256", TUYA_CLIENT_SECRET)
    .update(signStr)
    .digest("hex")
    .toUpperCase();
}

async function getAccessToken() {
  try {
    const now = Date.now();
    if (cachedToken && now < cachedTokenExpire) return cachedToken;

    const t = now.toString();
    const method = "GET";
    const url = "/v1.0/token?grant_type=1";
    const sign = genSignature({ method, url, t });

    const res = await axios.get(`${BASE_URL}${url}`, {
      headers: {
        client_id: TUYA_CLIENT_ID,
        sign,
        t,
        sign_method: "HMAC-SHA256",
      },
      timeout: 10000, // 10 second timeout
    });

    cachedToken = res.data.result.access_token;
    cachedTokenExpire = now + res.data.result.expire_time - 60 * 1000; // refresh 1 min before expiry
    return cachedToken;
  } catch (error) {
    console.error("Error getting access token:", error.message);
    if (error.code === 'ECONNABORTED') {
      throw new Error('Token request timeout - Tuya API is not responding');
    }
    if (error.code === 'ECONNRESET' || error.code === 'ECONNREFUSED') {
      throw new Error('Token connection failed - Network or Tuya API issue');
    }
    throw error;
  }
}

export async function fetchDeviceStatus(deviceId) {
  try {
    const token = await getAccessToken();
    const t = Date.now().toString();
    const method = "GET";
    const urlPath = `/v1.0/devices/${deviceId}/status`;
    const sign = genSignature({ method, url: urlPath, t, accessToken: token });

    const res = await axios.get(`${BASE_URL}${urlPath}`, {
      headers: {
        client_id: TUYA_CLIENT_ID,
        sign,
        t,
        sign_method: "HMAC-SHA256",
        access_token: token,
      },
      timeout: 10000, // 10 second timeout
    });

    return res.data.result; // returns array of status
  } catch (error) {
    console.error("Error in fetchDeviceStatus:", error.message);
    if (error.code === 'ECONNABORTED') {
      throw new Error('Request timeout - Tuya API is not responding');
    }
    if (error.code === 'ECONNRESET' || error.code === 'ECONNREFUSED') {
      throw new Error('Connection failed - Network or Tuya API issue');
    }
    if (error.response) {
      console.error("Response status:", error.response.status);
      console.error("Response data:", JSON.stringify(error.response.data, null, 2));
    }
    throw error;
  }
}

export async function controlDeviceSwitch(deviceId, switchState) {
  try {
    const token = await getAccessToken();
    const t = Date.now().toString();
    const method = "POST";
    const urlPath = `/v1.0/devices/${deviceId}/commands`;

    // Prepare the request body for standard device control
    const body = JSON.stringify({
      commands: [
        {
          code: "switch_1",
          value: switchState
        }
      ]
    });

    console.log(`Making request to: ${BASE_URL}${urlPath}`);
    console.log(`Request body: ${body}`);

    const sign = genSignature({ method, url: urlPath, body, t, accessToken: token });

    const res = await axios.post(`${BASE_URL}${urlPath}`, body, {
      headers: {
        client_id: TUYA_CLIENT_ID,
        sign,
        t,
        sign_method: "HMAC-SHA256",
        access_token: token,
        "Content-Type": "application/json",
      },
    });

    console.log(`Tuya API response status: ${res.status}`);
    console.log(`Tuya API response data:`, JSON.stringify(res.data, null, 2));

    return res.data;
  } catch (error) {
    console.error("Error in controlDeviceSwitch:", error.message);
    if (error.response) {
      console.error("Response status:", error.response.status);
      console.error("Response data:", JSON.stringify(error.response.data, null, 2));
    }
    throw error;
  }
}
