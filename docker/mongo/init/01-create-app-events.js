// Creates app.app_events. MongoDB runs this only when its data volume is empty.
const app = db.getSiblingDB("app");
if (!app.getCollectionNames().includes("app_events")) {
  app.createCollection("app_events");
}
print("init: collection app.app_events ready");
