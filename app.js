// Cheese Cave service app

require('dotenv').config();
'use strict';

const chalk = require('chalk');
console.log(chalk.yellow('Cheese Cave Operator: the back-end service app'));

// The connection string for the IoT Hub service.
const connectionString = process.env.SERVICE_CONNECTION_STRING;

// The device ID.
const deviceId = process.env.DEVICE_ID;

// The sample connects to service-side endpoint to call direct methods on devices.
const Client = require('azure-iothub').Client;
const Registry = require('azure-iothub').Registry;

// Connect to the service-side endpoint on your IoT hub.
const client = Client.fromConnectionString(connectionString);

// The sample connects to an IoT hub's Event Hubs-compatible endpoint to read messages sent from a device.
const EventHubClient = require('@azure/event-hubs').Client;
const EventPosition = require('@azure/event-hubs').EventPosition;
// const { EventHubClient, EventPosition } = require('@azure/event-hubs');

let eventHubClient;

function greenMessage(text) {
    console.log(chalk.green(text));
}

function redMessage(text) {
    console.log(chalk.red(text));
}

function printError(err) {
    redMessage(err.message);
};

// Display the message content - telemetry and properties.
function printMessage(message) {

    greenMessage('Telemetry received: ' + JSON.stringify(message.body));
    if (message.applicationProperties.fanAlert == 'true') {
        redMessage('Fan alert');
    }
    if (message.applicationProperties.temperatureAlert == 'true') {
        redMessage('Temperature alert');
    }
    if (message.applicationProperties.humidityAlert == 'true') {
        redMessage('Humidity alert');
    }
    console.log('');
};

// Connect to the partitions on the IoT Hub's Event Hubs-compatible endpoint.
EventHubClient.createFromIotHubConnectionString(connectionString).then(function (client) {
    greenMessage("Successfully created the EventHub Client from IoT Hub connection string.");

    // Save the client as a global variable.
    eventHubClient = client;

    return eventHubClient.getPartitionIds();

}).then(function (ids) {
    console.log("The partition ids are: ", ids);
    console.log('');
    return ids.map(function (id) {
        return eventHubClient.receive(id, printMessage, printError, { eventPosition: EventPosition.fromEnqueuedTime(Date.now()) });
    });
}).catch(printError);

const methodParams = {
    methodName: 'SetFanState',
    payload: 'on',
    responseTimeoutInSeconds: 30
};

function sendDirectMethod() {
    client.invokeDeviceMethod(deviceId, method, function (err, result) {
        if (err) {
            redMessage('Failed to invoke method \'' + methodParams.methodName + '\': ' + err.message);
        } else {
            greenMessage('Response from ' + methodParams.methodName + ' on ' + deviceId + ':');
            greenMessage(JSON.stringify(result, null, 2));
        }
    });
}

// Send a direct method to turn the fan on
sendDirectMethod();

// Locate the device twin via the Registry, then update some tags and properties.
const registry = Registry.fromConnectionString(connectionString);

registry.getTwin(deviceId, function (err, twin) {
    if (err) {
        redMessage(err.constructor.name + ': ' + err.message);
    } else {
        const desiredTemp = 50;
        const desiredHumidity = 85;
        const setDesiredValues = {
            // Tags aren't shared with the device, they are known only to the IoT hub.
            tags: {
                customerID: 'Customer1',
                cellar: 'Cellar1'
            },

            // Properties are shared with the device.
            properties: {
                desired: {
                    patchId: 'Set values',
                    temperature: desiredTemp.toString(),
                    humidity: desiredHumidity.toString()
                }
            }
        };

        // Update the device twin.
        twin.update(setDesiredValues, function (err) {
            if (err) {
                redMessage('Could not update twin: ' + err.constructor.name + ': ' + err.message);
            } else {
                greenMessage(twin.deviceId + ' twin updated successfully');

                // Show how a query to the device twins is handled.
                queryTwins();
            }
        });
    }
});

function queryTwins() {
    // Send a SQL query, to determine all the devices in "Cellar1".
    const query = registry.createQuery("SELECT * FROM devices WHERE tags.cellar = 'Cellar1'", 100);
    query.nextAsTwin(function (err, results) {
        if (err) {
            redMessage('Failed to fetch the results: ' + err.message);
        } else {
            greenMessage("Devices in Cellar1: " + results.map(function (twin) {
                return twin.deviceId
            }).join(','));
        }
    });
}