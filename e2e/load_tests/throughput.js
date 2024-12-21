import http from "k6/http";
import { check, sleep } from "k6";

const url = __ENV.URL;
const headers = {
  Connection: "keep-alive",
  "Content-Type": "application/json",
  Authorization: `Bearer ${__ENV.TOKEN || "default_token"}`,
};

const payload = JSON.stringify({
  data: "x".repeat(1024 * 1024 * 2), // 2MB payload
});

export let options = {
  stages: [
    { duration: "30s", target: 100 }, // Ramp-up to 100 VUs in 30 seconds
    { duration: "1m", target: 100 }, // Stay at 100 VUs for 1 minute
    { duration: "30s", target: 0 }, // Ramp-down to 0 VUs in 30 seconds
  ],
};

export default function () {
  const res = http.post(url, payload, { headers });
  check(res, {
    "status is 200": (r) => r.status === 200,
  });

  sleep(0.01); // Adjust this to control request rate
}
