import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

import { ICommandPalette } from '@jupyterlab/apputils';
import { ILauncher } from '@jupyterlab/launcher';
import { terminalIcon } from '@jupyterlab/ui-components';

import { ISettingRegistry } from '@jupyterlab/settingregistry';

import { ITerminal } from '@jupyterlab/terminal';

import {
  CustomTerminalManager,
  WaitingTerminalWidget
} from './reverseterminal';
import { CustomTerminal } from './reverseterminal';
import { ITranslator } from '@jupyterlab/translation';
import { listSystems } from './handler';

/**
 * Initialization data for the jupyterlabunicoreterminal extension.
 */

// const FACTORY = 'Editor';
const PALETTE_CATEGORY = 'UNICORE Terminals';

const plugin: JupyterFrontEndPlugin<void> = {
  id: 'jupyterlabunicoreterminal:plugin',
  description: 'A JupyterLab extension to add UNICORE reverse shells.',
  autoStart: true,
  requires: [ILauncher, ISettingRegistry],
  optional: [ICommandPalette],
  activate: activate
};

async function activate(
  app: JupyterFrontEnd,
  launcher: ILauncher | null,
  settingRegistry: ISettingRegistry,
  palette: ICommandPalette | null,
  translator: ITranslator | undefined
): Promise<void> {
  console.log('JupyterLab extension jupyterlabunicoreterminal is activated!2');
  const { commands } = app;
  let waitTerminal: WaitingTerminalWidget;
  let shellTerminal: CustomTerminal;

  const options: Partial<ITerminal.IOptions> = {};

  /**
   * Update the cached option values.
   */
  function updateOptions(settings: ISettingRegistry.ISettings): void {
    const composite = settings.composite as Partial<ITerminal.IOptions>;
    for (const key in composite) {
      if (Object.prototype.hasOwnProperty.call(composite, key)) {
        (options as any)[key] = composite[key as keyof ITerminal.IOptions];
      }
    }
  }

  console.log(launcher);
  console.log(settingRegistry);

  settingRegistry
    .load('@jupyterlab/terminal-extension:plugin')
    .then(settings => {
      updateOptions(settings);
      updateTerminals();
      settings.changed.connect(() => {
        updateOptions(settings);
        updateTerminals();
      });
    });

  function updateTerminals(): void {
    if (waitTerminal) {
      Object.keys(options).forEach(key => {
        const typedKey = key as keyof ITerminal.IOptions;
        waitTerminal.setOption(typedKey, options[typedKey]);
      });
    }
    if (shellTerminal) {
      Object.keys(options).forEach(key => {
        const typedKey = key as keyof ITerminal.IOptions;
        shellTerminal.setOption(typedKey, options[typedKey]);
      });
    }
  }

  const systems = await listSystems();
  for (const system of systems) {
    const command = `reverse-unicore-terminal-${system}:create`;
    commands.addCommand(command, {
      label: system,
      caption: `Start terminal on ${system}`,
      icon: args => (args['isPalette'] ? undefined : terminalIcon),
      execute: async args => {
        console.log(`Start ${system} terminal`);

        for (const widget of app.shell.widgets('main')) {
          if (
            widget.node.dataset.myCustomId ===
            `${system.toLowerCase}-terminal-001`
          ) {
            app.shell.activateById(widget.id);
            return;
          }
        }

        waitTerminal = new WaitingTerminalWidget(system, options, translator);
        waitTerminal.id = 'waiting-terminal';
        waitTerminal.title.icon = terminalIcon;
        waitTerminal.title.label = `Waiting (${system})...`;
        waitTerminal.title.closable = true;
        app.shell.add(waitTerminal, 'main');
        await waitTerminal.shellTermReady;

        const local_port = waitTerminal._port;
        const host = waitTerminal._host;

        const manager = new CustomTerminalManager(host, local_port);
        const session = await manager.startNew();
        shellTerminal = new CustomTerminal(
          `reverse-shell-terminal-${system}`,
          session,
          options,
          translator
        );

        shellTerminal.node.dataset.myCustomId = `${system.toLowerCase}-terminal-001`;
        shellTerminal.title.label = system;
        shellTerminal.title.closable = true;
        shellTerminal.title.icon = terminalIcon;

        app.shell.add(shellTerminal);
        app.shell.activateById(shellTerminal.id);
        // waitTerminal.close();
      }
    });
    // Add the command to the launcher
    if (launcher) {
      launcher.add({
        command,
        category: 'UNICORE Terminals',
        rank: 1
      });
    }

    // Add the command to the palette
    if (palette) {
      palette.addItem({
        command,
        args: { isPalette: true },
        category: PALETTE_CATEGORY
      });
    }
  }
}

export default plugin;
