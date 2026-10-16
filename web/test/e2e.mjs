// End-to-end: two headless Chrome tabs with fake camera/mic call each other
// through a running relay, then a third tab must be rejected.
//
//   cargo run -p moqi-relay &        # relay on :4443 (UDP) and :8080 (HTTP)
//   npm install && npm run test:e2e
//
// Env: CHROME_PATH (default /usr/bin/google-chrome), BASE_URL, RELAY_URL, SCREENSHOTS=dir

import assert from "node:assert/strict";
import puppeteer from "puppeteer-core";

const BASE = process.env.BASE_URL ?? "http://localhost:8080";
const relay = process.env.RELAY_URL ? `&relay=${encodeURIComponent(process.env.RELAY_URL)}` : "";
const room = `e2e${Date.now() % 1e6}`;
const shots = process.env.SCREENSHOTS;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const browser = await puppeteer.launch({
  executablePath: process.env.CHROME_PATH ?? "/usr/bin/google-chrome",
  headless: true,
  protocolTimeout: 30_000,
  args: [
    "--use-fake-ui-for-media-stream",
    "--use-fake-device-for-media-stream",
    "--auto-select-tab-capture-source-by-title=moqi",
    "--autoplay-policy=no-user-gesture-required",
    "--no-sandbox",
  ],
});

const step = (name) => console.log(`\n▶ ${name}`);
const ok = (msg) => console.log(`  ✓ ${msg}`);

async function join(name) {
  const page = await browser.newPage();
  await page.setViewport({ width: 1100, height: 700 });
  page.on("pageerror", (e) => console.log(`  [${name}] page error: ${e.message}`));
  page.on("console", (m) => m.type() === "error" && console.log(`  [${name}] ${m.text()}`));
  await page.goto(`${BASE}/?room=${room}${relay}`);
  await page.$eval("#name", (el, v) => (el.value = v), name);
  await page.click("#join-btn");
  return page;
}

// Background tabs don't render, so focus before interacting or measuring.
async function stats(page) {
  await page.bringToFront();
  const before = await page.evaluate(() => moqi.call?.remote?.peer.stats());
  await sleep(2000);
  const after = await page.evaluate(() => moqi.call?.remote?.peer.stats());
  const rate = (k) => (after[k].frames - before[k].frames) / 2;
  return { video: rate("video"), screen: rate("screen"), audio: rate("audio"), lost: after.audio.lost };
}

const dom = (page) =>
  page.evaluate(() => ({
    remoteName: document.querySelector("[data-remote-name]").textContent,
    cameraVisible: document.getElementById("remote-placeholder").hidden,
    micOn: document.getElementById("remote-mic").classList.contains("on"),
    screenVisible: !document.getElementById("remote-screen-tile").hidden,
    waiting: document.getElementById("remote-waiting").textContent,
    canvas: [document.getElementById("remote-video").width, document.getElementById("remote-video").height],
  }));

try {
  step("Alice and Bob join");
  const alice = await join("Alice");
  await sleep(1000);
  const bob = await join("Bob");
  await sleep(5000);

  for (const [me, page, other] of [["Alice", alice, "Bob"], ["Bob", bob, "Alice"]]) {
    const s = await stats(page);
    const d = await dom(page);
    assert.equal(d.remoteName, other, `${me} sees ${other}'s name from the catalog`);
    assert.ok(d.cameraVisible && d.canvas[0] > 0, `${me} renders ${other}'s camera`);
    assert.ok(s.video >= 10, `${me} receives video (${s.video} fps)`);
    assert.ok(s.audio >= 40, `${me} receives audio datagrams (${s.audio}/s)`);
    ok(`${me} ← ${other}: video ${s.video} fps ${d.canvas.join("×")}, audio ${s.audio} frames/s, lost ${s.lost}`);
  }
  if (shots) await alice.screenshot({ path: `${shots}/call.png` });

  step("Alice shares her screen");
  await alice.bringToFront();
  await alice.click("#screen-btn");
  await sleep(3000);
  const s = await stats(bob);
  assert.ok((await dom(bob)).screenVisible, "Bob shows Alice's screen tile");
  assert.ok(s.screen >= 3, `Bob receives screen frames (${s.screen} fps)`);
  ok(`Bob ← Alice screen: ${s.screen} fps`);
  if (shots) await bob.screenshot({ path: `${shots}/screenshare.png` });

  step("Carol tries to join a full room");
  const carol = await join("Carol");
  await sleep(2500);
  const err = await carol.$eval("#join-error", (el) => el.textContent);
  assert.match(err, /full/i, "third participant is rejected");
  ok(`Carol rejected: "${err}"`);
  await carol.close();

  step("Bob mutes and turns his camera off");
  await bob.bringToFront();
  await bob.click("#mic-btn");
  await bob.click("#cam-btn");
  await sleep(1500);
  await alice.bringToFront();
  const d = await dom(alice);
  assert.ok(!d.micOn && !d.cameraVisible && d.waiting === "Camera off", "Alice sees Bob muted with camera off");
  ok("catalog update propagated");

  step("Alice stops sharing and leaves");
  await alice.click("#screen-btn");
  await alice.click("#leave-btn");
  await sleep(1500);
  await bob.bringToFront();
  const after = await dom(bob);
  assert.match(after.waiting, /waiting/i, "Bob is back to waiting");
  assert.ok(!after.screenVisible, "screen tile removed");
  ok("NAMESPACE_DONE handled; Bob is alone again");

  console.log("\nAll e2e checks passed.");
} finally {
  await browser.close();
}
