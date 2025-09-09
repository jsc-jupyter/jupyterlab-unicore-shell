import { TerminalManager } from '@jupyterlab/services';

import { ServerConnection } from '@jupyterlab/services';

export class CustomTerminalManager extends TerminalManager {
  constructor(host: string, local_port: string) {
    // Create custom serverSettings
    const serverSettings = ServerConnection.makeSettings({
      wsUrl: `ws://${host}:${local_port}`
    });

    super({ serverSettings });
  }
}
