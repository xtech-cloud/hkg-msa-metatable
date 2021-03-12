package config

const defaultYAML string = `
service:
    name: msa.api.hkg.metatable
    address: :18801
    ttl: 15
    interval: 10
logger:
    level: info
    dir: /var/log/msa/
`
