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
database:
    mongodb:
        address: localhost:27017
        timeout: 10
        user: root
        password: mongodb@OMO
        db: hkg_msa_metatable
`
