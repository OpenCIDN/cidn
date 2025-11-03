const containers = {
    processing: document.getElementById('processing-container'),
    pending: document.getElementById('pending-container'),
    failed: document.getElementById('failed-container'),
    completed: document.getElementById('completed-container')
};

const highlightBytes = (bytes) => {
    if (!bytes || bytes < 0) return '<span class="size-unit-bytes">0 Bytes</span>';
    const k = 1024;
    const sizes = ['Bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    const sizeClasses = ['size-unit-bytes', 'size-unit-kib', 'size-unit-mib', 'size-unit-gib', 'size-unit-tib', 'size-unit-pib'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    const formattedValue = (bytes / k ** i).toFixed(i ? 2 : 0);
    return `<span class="${sizeClasses[i]}">${formattedValue} ${sizes[i]}</span>`;
};

const highlightError = error =>
    error ? `<span class="error-text">${error}</span>` : '';

const highlightProgress = (progress, total) =>
    total > 0 ? `<span title="Progress in bytes">${highlightBytes(progress)} / ${highlightBytes(total)}</span>` : `<span title="Progress in bytes">${highlightBytes(progress)}</span>`;

const highlightSpeed = (speed, remainingTime) =>
    speed ? `<span title="Estimated remaining time, Download speed">
        ${remainingTime ? `${svgIcon('clock')} ${formatRemainingTime(remainingTime)}` : ''}
        ${svgIcon('download')} ${highlightBytes(speed)} / s
        </span>` : '<span></span>';

const svgIcon = (name) =>
    `<svg width="16" height="16" style="vertical-align: middle;"><use xlink:href="#icon-${name}"></use></svg>`;

const MAX_INDIVIDUAL_ICONS = 8;

const highlightChunksInfo = data => {
    const pending = data.pendingChunks || 0;
    const running = data.runningChunks || 0;
    const succeeded = data.succeededChunks || 0;
    const failed = data.failedChunks || 0;
    const chunksNumber = data.chunksNumber || 0;
    const idle = chunksNumber - pending - running - succeeded - failed;

    const icons = [
        { type: 'idle', count: idle },
        { type: 'pending', count: pending },
        { type: 'failed', count: failed },
        { type: 'running', count: running },
        { type: 'succeeded', count: succeeded }
    ];

    const iconHTML = chunksNumber > MAX_INDIVIDUAL_ICONS
        ? icons.filter(i => i.count > 0).map(i => `${svgIcon(i.type)}<span>${i.count}</span>`).join(' ')
        : icons.map(i => svgIcon(i.type).repeat(i.count)).join('');

    return `<span title="Chunks status: idle, pending, failed, running, succeeded / total">
    ${iconHTML} <span>/ ${chunksNumber}</span>
    </span>`;
}

const progressData = {};
const statusMap = {
    'Pending': 'pending',
    'Unknown': 'pending',
    'Running': 'processing',
    'Succeeded': 'completed',
    'Failed': 'failed'
};

const clearDataItem = (event) => {
    document.getElementById(event.lastEventId)?.remove();
};

const dataItem = elementId => {
    const element = document.createElement('div');
    element.id = elementId;
    element.className = 'data-item';
    // Add group styling if this is a group aggregate
    if (elementId.startsWith('group-')) {
        element.classList.add('data-item-group');
    }
    element.style.animation = 'item-entrance 0.3s';
    return element;
};

const renderDataItem = (event) => {
    const elementId = event.lastEventId;
    const element = document.getElementById(elementId) || dataItem(elementId);
    const data = JSON.parse(event.data);
    const targetContainer = containers[statusMap[data.phase]];

    if (element.parentNode !== targetContainer) {
        element.remove();
    }

    element.innerHTML = generateItemContent(data, elementId);
    if (!element.isConnected) {
        targetContainer.prepend(element);
    }
};

const generateItemContent = (data, elementId) => {
    const displayName = data.displayName || data.name;
    const size = data.total || 0;
    const progress = data.progress || 0;
    const errors = data.errors || [];
    const hasChunks = data.chunksNumber && (data.pendingChunks || data.runningChunks || data.succeededChunks || data.failedChunks);
    const isGroup = elementId.startsWith('group-');

    const iconMap = {
        'Pending': 'pending',
        'Unknown': 'pending',
        'Running': 'running',
        'Succeeded': 'succeeded',
        'Failed': 'failed'
    };
    let linkInfo = '';
    if (data.members && data.members.length > 0) {
        const memberLinks = data.members.map(member =>
            `<a href="#${member.uid}">${svgIcon(iconMap[member.phase])}${member.displayName}</a>`
        ).join(' ');
        linkInfo = `<span class="link-info">${memberLinks}</span>`;
    } else if (data.group) {
        const groupId = 'group-' + data.group;
        linkInfo = `<span class="link-info"><a href="#${groupId}">${data.group}</a></span>`;
    }

    const header = `
        <div class="data-header">
            ${displayName}
            <a href="#${elementId}" class="anchor-link">#</a>
        </div>
        ${linkInfo}
    `;

    switch (data.phase) {
        case 'Running': {
            let speed = 0;
            let remainingTime = 0;
            const now = Date.now();
            if (progressData[data.name]) {
                const prev = progressData[data.name];
                const timeDiff = (now - prev.time) / 1000;
                const progressDiff = progress - prev.progress;
                if (timeDiff > 0 && progressDiff > 0) {
                    speed = progressDiff / timeDiff;
                    if (size > 0) {
                        remainingTime = (size - progress) / speed;
                        if (prev.remainingTime) {
                            remainingTime = (remainingTime * 9 + prev.remainingTime) / 10;
                        }
                        remainingTime = Math.round(remainingTime);
                    }
                }
            }
            progressData[data.name] = { progress, remainingTime, time: now };

            let progressPercent = size > 0 ? (progress / size * 100).toFixed(0) : 0;
            if (progressPercent > 99 && size != progress) {
                progressPercent = 99;
            }

            return `${header}
            <div class="progress-text">
                ${size > 0 ? highlightProgress(progress, size) : `<span></span>`}
                ${highlightSpeed(speed, remainingTime)} 
                ${hasChunks ? highlightChunksInfo(data) : `<span></span>`}
            </div>
            ${size > 0 ? `<div class="progress-container"><div class="progress-bar" style="width: ${progressPercent}%"></div></div>` : ''}`;
        }
        case 'Failed':
            return `${header}
            ${highlightError(escapeErrors(errors))}
            <div class="progress-text">
                ${size > 0 ? highlightProgress(progress, size) : `<span></span>`}
                ${hasChunks ? highlightChunksInfo(data) : `<span></span>`}
            </div>`;
        case 'Pending':
        case 'Unknown':
            return `${header}
            <div class="progress-text">
                ${size > 0 ? highlightProgress(progress, size) : `<span></span>`}
                ${hasChunks ? highlightChunksInfo(data) : `<span></span>`}
            </div>`;
        case 'Succeeded':
            return `${header}
            <div class="progress-text">
                ${highlightBytes(size)}
                ${hasChunks ? highlightChunksInfo(data) : `<span></span>`}
            </div>`;
    }
};

const escapeErrors = errors => {
    if (!errors || errors.length === 0) return '';
    return errors.map(err => escapeHtml(err)).join('<br>');
};

const escapeHtml = unsafe => {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return unsafe.replace(/[&<>"']/g, m => map[m]);
};

const toggleContainer = containerId => {
    const container = document.getElementById(containerId);
    const title = container.previousElementSibling;
    container.style.display = container.style.display === 'none' ? 'block' : 'none';
    title.classList.toggle('collapsed');
};

const checkAndExpandTargetContainer = () => {
    const hash = window.location.hash;
    if (!hash) return;

    document.querySelectorAll('.data-item-highlighted').forEach(el => {
        el.classList.remove('data-item-highlighted');
    });

    const targetElement = document.getElementById(hash.substring(1));
    if (!targetElement) return;


    targetElement.classList.add('data-item-highlighted');

    const container = targetElement.closest('.status-container');
    if (!container) return;

    if (container.style.display === 'none') {
        container.style.display = 'block';
        const title = container.previousElementSibling;
        title.classList.remove('collapsed');
    }

    setTimeout(() => {
        targetElement.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }, 1000);
};

window.addEventListener('hashchange', checkAndExpandTargetContainer);

const showError = message => {
    const errorDiv = document.getElementById('error');
    errorDiv.style.display = 'block';
    errorDiv.textContent = message;
    setTimeout(() => {
        const errorModal = document.createElement('div');
        errorModal.style.position = 'fixed';
        errorModal.style.top = '20%';
        errorModal.style.left = '50%';
        errorModal.style.transform = 'translateX(-50%)';
        errorModal.style.backgroundColor = 'var(--color-alert-error)';
        errorModal.style.color = 'white';
        errorModal.style.padding = '20px';
        errorModal.style.borderRadius = '5px';
        errorModal.style.boxShadow = '0 4px 8px rgba(0,0,0,0.2)';
        errorModal.style.zIndex = '1000';
        errorModal.style.textAlign = 'center';
        errorModal.style.fontWeight = 'bold';
        errorModal.innerHTML = `
            <div style="position: absolute; top: 5px; right: 5px; cursor: pointer;" onclick="this.parentNode.remove()">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2">
                    <line x1="18" y1="6" x2="6" y2="18"></line>
                    <line x1="6" y1="6" x2="18" y2="18"></line>
                </svg>
            </div>
            ${message}
            <div style="display: flex; gap: 10px; justify-content: center; margin: 10px auto;">
                <button onclick="location.reload()" 
                        style="background: white; 
                               color: var(--color-alert-error); 
                               border: none; 
                               padding: 8px 16px; 
                               border-radius: 4px; 
                               cursor: pointer;
                               margin: 0 auto;">
                    Refresh Page
                </button>
            </div>
        `;
        document.body.appendChild(errorModal);
    }, 100);
};

const handleRenderEvent = (event) => {
    try {
        renderDataItem(event);
    } catch (e) {
        console.error('Error parsing event data:', e);
        showError(`Error parsing event data: ${e.message}`);
    }

    const hash = window.location.hash;
    if (hash && event.lastEventId === hash.substring(1)) {
        checkAndExpandTargetContainer();
    }
};

const startStream = async () => {
    try {
        const eventSource = new EventSource('/api/events');

        eventSource.addEventListener('ADD', handleRenderEvent);
        eventSource.addEventListener('UPDATE', handleRenderEvent);

        eventSource.addEventListener('DELETE', (event) => {
            try {
                clearDataItem(event);
            } catch (e) {
                console.error('Error parsing event data:', e);
                showError(`Error parsing event data: ${e.message}`);
            }
        });

        eventSource.onerror = (e) => {
            console.error('EventSource error:', e);
            showError('Connection error occurred');
            eventSource.close();
        };
    } catch (e) {
        console.error('EventSource initialization error:', e);
        showError(`EventSource initialization error: ${e.message}`);
    }
};

const formatRemainingTime = remainingTime => {
    const hours = Math.floor(remainingTime / 3600);
    const minutes = Math.floor((remainingTime % 3600) / 60);
    const seconds = remainingTime % 60;

    return [
        hours > 0 ? `<span class="time-unit-h">${hours} h</span>` : null,
        minutes > 0 ? `<span class="time-unit-m">${minutes} m</span>` : null,
        `<span class="time-unit-s">${seconds} s</span>`
    ].filter(Boolean).join(' ');
};

document.addEventListener('DOMContentLoaded', () => {
    startStream();
});

Object.values(containers).forEach(container => {
    const title = container.previousElementSibling;
    if (!title) return;

    const countSpan = title.querySelector('.status-status-item-count');
    if (!countSpan) return;

    const observer = new MutationObserver(() => {
        countSpan.textContent = `[${container.children.length}]`;
    });
    observer.observe(container, { childList: true });
});
