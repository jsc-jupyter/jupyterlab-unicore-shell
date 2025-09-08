import { TerminalManager, Terminal as TerminalNS } from '@jupyterlab/services';
import { Terminal, ITerminal } from '@jupyterlab/terminal';

import { ServerConnection } from '@jupyterlab/services';

export class CustomTerminalManager extends TerminalManager {
  constructor(
    host: string,
    local_port: string,
    options: TerminalManager.IOptions = {}
  ) {
    // Create custom serverSettings
    const serverSettings = ServerConnection.makeSettings({
      wsUrl: `ws://${host}:${local_port}`
    });

    super({ serverSettings });
  }
}

import { ITranslator } from '@jupyterlab/translation';

export class CustomTerminal extends Terminal {
  constructor(
    id: string,
    session: TerminalNS.ITerminalConnection,
    options: Partial<ITerminal.IOptions> = {},
    translator?: ITranslator
  ) {
    super(session, options, translator);
    this.id = id;
  }

  protected onCloseRequest(msg: any): void {
    // In this case, closing should be same as dispose
    super.onCloseRequest(msg);
    this.dispose();
  }
}
