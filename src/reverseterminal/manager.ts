import { URLExt } from '@jupyterlab/coreutils';
import { TerminalManager } from '@jupyterlab/services';

import { ServerConnection } from '@jupyterlab/services';

export class RemoteTerminalManager extends TerminalManager {
  constructor() {
    // Use remoteshell endpoints

    // behind a JupyterHub setup we need the user specific path /user/_user_/_server_/api.
    // That's what the defaultSettings will deliver.
    const defaultSettings = ServerConnection.makeSettings();
    const serverSettings = ServerConnection.makeSettings({
      baseUrl: URLExt.join(defaultSettings.baseUrl, '/remoteshell'),
      wsUrl: URLExt.join(defaultSettings.wsUrl, '/remoteshell')
    });

    super({ serverSettings });
  }
}
