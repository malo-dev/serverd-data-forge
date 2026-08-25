function formatUptime(ms) {
  const totalSeconds = Math.floor(ms / 1000)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  return `${hours}h ${minutes}m ${seconds}s`
}

fetch('/health', { headers: { Accept: 'application/json' } })
  .then((res) => res.json())
  .then((data) => {
    document.getElementById('stat-rooms').textContent = data.rooms
    document.getElementById('stat-devices').textContent = data.devices
    document.getElementById('stat-uptime').textContent = formatUptime(data.uptimeMs)
  })
  .catch(() => {
    document.getElementById('stat-rooms').textContent = '?'
    document.getElementById('stat-devices').textContent = '?'
    document.getElementById('stat-uptime').textContent = '?'
  })
