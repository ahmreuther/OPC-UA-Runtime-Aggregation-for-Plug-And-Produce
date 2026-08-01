# Security

No operational credential, deployment PKI, private laboratory address, packet capture, or runtime-state snapshot is intended to be part of this release.

The bundled `opcua` dependency contains explicitly documented upstream test-only keys. They are public fixtures and provide no security. Never reuse them for a deployment.

For a real installation:

- generate a fresh OPC UA application certificate and private key;
- keep `config.json`, PKI directories, API tokens, and logs outside version control;
- replace the loopback example address with the intended interface only in deployment-specific configuration;
- review source endpoints and discovery exposure before connecting a production network.

Please report a suspected release secret privately to the repository maintainers rather than opening a public issue containing the value.
