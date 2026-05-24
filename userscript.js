
if __name__ == "__main__":
    main()
// ==UserScript==
// @name         AI Bridge Connector v1.0
// @namespace    http://tampermonkey.net/
// @version      1.0.0
// @description  Connects You.com AI chat to local Bridge Server for live file editing, execution, and testing
// @match        https://you.com/*
// @grant        none
// ==/UserScript==

(function() {
'use strict';

// ════════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ════════════════════════════════════════════════════════════════════════════

var BRIDGE_WS_URL = 'ws://localhost:8765';
var BRIDGE_HTTP_URL = 'http://localhost:8766';
var RECONNECT_INTERVAL = 5000;
var HEARTBEAT_INTERVAL = 15000;
var MAX_RECONNECT_ATTEMPTS = 50;
var PANEL_WIDTH = 420;

// ════════════════════════════════════════════════════════════════════════════
// STATE
// ════════════════════════════════════════════════════════════════════════════

var ws = null;
var connected = false;
var reconnectAttempts = 0;
var reconnectTimer = null;
var heartbeatTimer = null;
var pendingRequests = {};
var requestIdCounter = 0;
var bridgeStatus = null;
var openFiles = {};
var executionOutputs = {};
var testResults = [];
var annotations = {};
var logEntries = [];
var panelVisible = false;
var activeTab = 'files';
var autoMode = false;
var autoModeConfig = {
    testCommand: '',
    maxIterations: 20,
    fixEnabled: true,
};

// ════════════════════════════════════════════════════════════════════════════
// UTILITY
// ════════════════════════════════════════════════════════════════════════════

function genId() {
    return 'req_' + (++requestIdCounter) + '_' + Date.now().toString(36);
}

function escapeHtml(text) {
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function truncate(str, max) {
    if (!str) return '';
    return str.length > max ? str.substring(0, max) + '...' : str;
}

function formatTime(isoStr) {
    if (!isoStr) return '';
    try {
        var d = new Date(isoStr);
        return d.toLocaleTimeString();
    } catch(e) { return isoStr; }
}

function addLog(level, message, details) {
    logEntries.push({
        timestamp: new Date().toISOString(),
        level: level,
        message: message,
        details: details || '',
    });
    if (logEntries.length > 200) logEntries.shift();
    renderPanel();
}

// ════════════════════════════════════════════════════════════════════════════
// WEBSOCKET CONNECTION
// ════════════════════════════════════════════════════════════════════════════

function connect() {
    if (ws && (ws.readyState === WebSocket.CONNECTING || ws.readyState === WebSocket.OPEN)) {
        return;
    }

    try {
        ws = new WebSocket(BRIDGE_WS_URL);
    } catch(e) {
        addLog('error', 'WebSocket creation failed: ' + e.message);
        scheduleReconnect();
        return;
    }

    ws.onopen = function() {
        connected = true;
        reconnectAttempts = 0;
        addLog('info', 'Connected to Bridge Server');
        updateConnectionUI(true);
        startHeartbeat();
        sendMessage('status', {});
    };

    ws.onmessage = function(event) {
        try {
            var msg = JSON.parse(event.data);
            handleServerMessage(msg);
        } catch(e) {
            addLog('error', 'Failed to parse message: ' + e.message);
        }
    };

    ws.onclose = function(event) {
        connected = false;
        updateConnectionUI(false);
        stopHeartbeat();
        if (event.code !== 1000) {
            addLog('warning', 'Connection closed (code: ' + event.code + ')');
            scheduleReconnect();
        }
    };

    ws.onerror = function(event) {
        addLog('error', 'WebSocket error');
        updateConnectionUI(false);
    };
}

function disconnect() {
    if (ws) {
        ws.close(1000, 'User disconnect');
        ws = null;
    }
    connected = false;
    stopHeartbeat();
    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
    updateConnectionUI(false);
}

function scheduleReconnect() {
    if (reconnectTimer) return;
    if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
        addLog('error', 'Max reconnect attempts reached. Click to retry.');
        return;
    }
    reconnectAttempts++;
    var delay = Math.min(RECONNECT_INTERVAL * Math.pow(1.5, reconnectAttempts - 1), 30000);
    reconnectTimer = setTimeout(function() {
        reconnectTimer = null;
        connect();
    }, delay);
}

function startHeartbeat() {
    stopHeartbeat();
    heartbeatTimer = setInterval(function() {
        if (connected) {
            sendMessage('ping', {});
        }
    }, HEARTBEAT_INTERVAL);
}

function stopHeartbeat() {
    if (heartbeatTimer) {
        clearInterval(heartbeatTimer);
        heartbeatTimer = null;
    }
}

// ════════════════════════════════════════════════════════════════════════════
// MESSAGE SENDING
// ════════════════════════════════════════════════════════════════════════════

function sendMessage(type, payload, callback) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        if (callback) callback({error: 'Not connected'});
        addLog('error', 'Cannot send: not connected');
        return null;
    }

    var id = genId();
    var msg = {
        type: type,
        id: id,
        payload: payload || {},
    };

    if (callback) {
        pendingRequests[id] = {
            callback: callback,
            timestamp: Date.now(),
            type: type,
        };
    }

    try {
        ws.send(JSON.stringify(msg));
    } catch(e) {
        addLog('error', 'Send failed: ' + e.message);
        delete pendingRequests[id];
        if (callback) callback({error: e.message});
        return null;
    }

    return id;
}

// ════════════════════════════════════════════════════════════════════════════
// MESSAGE HANDLING
// ════════════════════════════════════════════════════════════════════════════

function handleServerMessage(msg) {
    var type = msg.type;
    var id = msg.id;
    var payload = msg.payload || {};

    // Handle response to pending request
    if ((type === 'response' || type === 'error') && id && pendingRequests[id]) {
        var req = pendingRequests[id];
        delete pendingRequests[id];
        if (req.callback) {
            req.callback(type === 'error' ? {error: payload} : payload);
        }
        if (type === 'error') {
            addLog('error', 'Request failed [' + req.type + ']: ' + (payload.message || JSON.stringify(payload)));
        }
        renderPanel();
        return;
    }

    // Handle streaming messages
    switch(type) {
        case 'stream_stdout':
            handleStreamOutput(payload, 'stdout');
            break;
        case 'stream_stderr':
            handleStreamOutput(payload, 'stderr');
            break;
        case 'stream_exit':
            handleStreamExit(payload);
            break;
        case 'file_changed':
            handleFileChanged(payload);
            break;
        case 'test_iteration':
            handleTestIteration(payload);
            break;
        case 'test_result':
            handleTestResult(payload);
            break;
        case 'log':
            handleServerLog(payload);
            break;
        case 'error':
            addLog('error', payload.message || 'Unknown error');
            break;
    }

    renderPanel();
}

function handleStreamOutput(payload, streamType) {
    var execId = payload.exec_id;
    if (!executionOutputs[execId]) {
        executionOutputs[execId] = {stdout: '', stderr: '', status: 'running', startTime: Date.now()};
    }
    if (streamType === 'stdout') {
        executionOutputs[execId].stdout += payload.data;
    } else {
        executionOutputs[execId].stderr += payload.data;
    }
    renderPanel();
}

function handleStreamExit(payload) {
    var execId = payload.exec_id;
    if (!executionOutputs[execId]) {
        executionOutputs[execId] = {stdout: '', stderr: '', status: 'completed', startTime: Date.now()};
    }
    executionOutputs[execId].exitCode = payload.exit_code;
    executionOutputs[execId].duration = payload.duration;
    executionOutputs[execId].status = payload.status;
    addLog('info', 'Exec [' + execId + '] finished: exit=' + payload.exit_code + ' (' + payload.duration + 's)');
}

function handleFileChanged(payload) {
    addLog('info', 'File changed: ' + payload.path + ' (' + payload.change_type + ')');
    // Refresh file if it's open
    if (openFiles[payload.path]) {
        readFile(payload.path);
    }
}

function handleTestIteration(payload) {
    testResults.push(payload);
    var status = payload.passed ? '[PASS]' : '[FAIL]';
    addLog('info', 'Test #' + payload.iteration + '/' + payload.max_iterations + ' ' + status + ' (exit=' + payload.exit_code + ')');
}

function handleTestResult(payload) {
    addLog(payload.passed ? 'info' : 'warning',
        'Test loop ' + (payload.passed ? 'PASSED' : 'FAILED') +
        ' after ' + payload.total_iterations + ' iterations');
}

function handleServerLog(payload) {
    addLog(payload.level || 'info', payload.message, payload.details);
}

// ════════════════════════════════════════════════════════════════════════════
// API FUNCTIONS (used by the AI and the UI)
// ════════════════════════════════════════════════════════════════════════════

function readFile(path, callback) {
    sendMessage('file_read', {path: path}, function(resp) {
        if (resp.error) {
            addLog('error', 'Read failed: ' + (resp.error.message || JSON.stringify(resp.error)));
        } else if (resp.data) {
            openFiles[resp.data.path] = resp.data;
            addLog('info', 'Read: ' + path + ' (' + resp.data.lines + ' lines)');
        }
        if (callback) callback(resp);
        renderPanel();
    });
}

function writeFile(path, content, callback) {
    sendMessage('file_write', {path: path, content: content}, function(resp) {
        if (resp.error) {
            addLog('error', 'Write failed: ' + (resp.error.message || JSON.stringify(resp.error)));
        } else if (resp.data) {
            addLog('info', 'Written: ' + path + ' (' + resp.data.lines + ' lines)');
        }
        if (callback) callback(resp);
        renderPanel();
    });
}

function searchFiles(query, options, callback) {
    var payload = Object.assign({query: query}, options || {});
    sendMessage('file_search', payload, function(resp) {
        if (resp.data) {
            addLog('info', 'Search "' + query + '": ' + resp.data.total + ' results');
        }
        if (callback) callback(resp);
    });
}

function replaceInFile(path, search, replace, options, callback) {
    var payload = Object.assign({path: path, search: search, replace: replace}, options || {});
    sendMessage('file_replace', payload, function(resp) {
        if (resp.data) {
            addLog('info', 'Replace in ' + path + ': ' + resp.data.replacements + ' replacements');
        }
        if (callback) callback(resp);
    });
}

function executeCommand(command, options, callback) {
    var payload = Object.assign({command: command}, options || {});
    sendMessage('exec_run', payload, function(resp) {
        if (resp.data) {
            addLog('info', 'Exec started: ' + resp.data.exec_id);
        }
        if (callback) callback(resp);
    });
}

function runTestLoop(command, options, callback) {
    var payload = Object.assign({command: command}, options || {});
    sendMessage('test_loop', payload, function(resp) {
        if (resp.data) {
            addLog('info', 'Test loop started');
        }
        if (callback) callback(resp);
    });
}

function installDeps(dependencies, language, callback) {
    sendMessage('env_install', {dependencies: dependencies, language: language || 'python'}, function(resp) {
        if (resp.data) {
            addLog('info', 'Installing: ' + dependencies.join(', '));
        }
        if (callback) callback(resp);
    });
}

function annotateFile(path, lineStart, lineEnd, text, callback) {
    sendMessage('file_annotate', {
        path: path, line_start: lineStart, line_end: lineEnd, text: text
    }, function(resp) {
        if (resp.data) {
            addLog('info', 'Annotated ' + path + ' L' + lineStart + '-' + lineEnd);
        }
        if (callback) callback(resp);
    });
}

// ════════════════════════════════════════════════════════════════════════════
// HARVEST INTEGRATION
// ════════════════════════════════════════════════════════════════════════════

function harvestAndSend() {
    // Get the accumulated code from Auto-Coder
    var code = '';
    if (window.acl_debug && window.acl_debug.getAccumulated) {
        code = window.acl_debug.getAccumulated();
    }
    if (!code || code.trim().length < 50) {
        // Try harvesting from DOM
        if (window.acl_debug && window.acl_debug.harvest) {
            code = window.acl_debug.harvest();
        }
    }
    if (!code || code.trim().length < 50) {
        addLog('warning', 'No code to send. Harvest first or write code.');
        return;
    }

    // Parse the code for bridge commands
    var commands = parseBridgeCommands(code);
    if (commands.length > 0) {
        executeBridgeCommands(commands);
    } else {
        // Default: try to detect file path and write
        addLog('info', 'No bridge commands found. Sending as raw code.');
        promptFileWrite(code);
    }
}

function parseBridgeCommands(code) {
    // Look for JSON bridge command blocks in the code
    // Format: // @bridge: {"type": "file_write", "payload": {...}}
    var commands = [];
    var lines = code.split('\n');
    var jsonBuffer = '';
    var inJsonBlock = false;

    for (var i = 0; i < lines.length; i++) {
        var line = lines[i];

        // Single-line command
        var match = line.match(/\/\/\s*@bridge:\s*(.+)/);
        if (match) {
            try {
                commands.push(JSON.parse(match[1]));
            } catch(e) {
                // Might be multi-line
            }
            continue;
        }

        // Multi-line JSON block
        if (line.trim() === '// @bridge-start') {
            inJsonBlock = true;
            jsonBuffer = '';
            continue;
        }
        if (line.trim() === '// @bridge-end' && inJsonBlock) {
            inJsonBlock = false;
            try {
                var parsed = JSON.parse(jsonBuffer);
                if (Array.isArray(parsed)) {
                    commands = commands.concat(parsed);
                } else {
                    commands.push(parsed);
                }
            } catch(e) {
                addLog('error', 'Failed to parse bridge JSON block: ' + e.message);
            }
            continue;
        }
        if (inJsonBlock) {
            // Strip leading // if present
            jsonBuffer += line.replace(/^\s*\/\/\s?/, '') + '\n';
        }
    }

    return commands;
}

function executeBridgeCommands(commands) {
    addLog('info', 'Executing ' + commands.length + ' bridge commands...');
    for (var i = 0; i < commands.length; i++) {
        var cmd = commands[i];
        sendMessage(cmd.type, cmd.payload || {}, function(resp) {
            if (resp.error) {
                addLog('error', 'Command failed: ' + JSON.stringify(resp.error));
            }
        });
    }
}

function promptFileWrite(code) {
    // Show a simple prompt for file path
    var path = prompt('Enter file path to write (relative to allowed dir):');
    if (path) {
        writeFile(path, code);
    }
}

// ════════════════════════════════════════════════════════════════════════════
// AUTO-MODE: AI iterates until tests pass
// ════════════════════════════════════════════════════════════════════════════

function startAutoMode() {
    if (!autoModeConfig.testCommand) {
        addLog('error', 'Set test command first');
        return;
    }
    autoMode = true;
    addLog('info', 'Auto-mode started. Test: ' + autoModeConfig.testCommand);
    runTestLoop(autoModeConfig.testCommand, {
        max_iterations: autoModeConfig.maxIterations,
    });
    renderPanel();
}

function stopAutoMode() {
    autoMode = false;
    sendMessage('test_
