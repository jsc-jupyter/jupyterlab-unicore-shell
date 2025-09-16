import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

import {
  ICommandPalette,
  IThemeManager,
  WidgetTracker
} from '@jupyterlab/apputils';
import { ILauncher } from '@jupyterlab/launcher';
import { terminalIcon } from '@jupyterlab/ui-components';

import { ISettingRegistry } from '@jupyterlab/settingregistry';

import { ITerminal } from '@jupyterlab/terminal';

import { ITranslator } from '@jupyterlab/translation';
import { listSystems, deleteShell, listOpenSessions } from './handler';
import { LazyTerminal } from './reverseterminal';

/**
 * Initialization data for the jupyterlabunicoreterminal extension.
 */

const PALETTE_CATEGORY = 'UNICORE Terminals';

const plugin: JupyterFrontEndPlugin<void> = {
  id: 'jupyterlab-unicore-shell:plugin',
  description: 'A JupyterLab extension to add UNICORE reverse shells.',
  autoStart: true,
  requires: [ILauncher, ISettingRegistry, IThemeManager],
  optional: [ICommandPalette],
  activate: activate
};

async function activate(
  app: JupyterFrontEnd,
  launcher: ILauncher | null,
  settingRegistry: ISettingRegistry,
  thememanager: IThemeManager,
  palette: ICommandPalette | null,
  translator: ITranslator | undefined
): Promise<void> {
  console.log('JupyterLab extension jupyterlab-unicore-shell is activated!');
  const { commands } = app;
  const terminalTracker = new WidgetTracker<LazyTerminal>({
    namespace: 'jupyterlabunicoreshell:terminals'
  });

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

  thememanager.themeChanged.connect(() => {
    updateTerminals();
  });

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
    terminalTracker.forEach(terminal => {
      Object.keys(options).forEach(key => {
        const typedKey = key as keyof ITerminal.IOptions;
        terminal.setOption(typedKey, options[typedKey]);
      });
    });
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
            `${system.toLowerCase()}-terminal-001`
          ) {
            if (!(widget instanceof LazyTerminal)) {
              // close placeholder widget
              widget.close();
              break;
            }
            const current = app.shell.currentWidget;
            if (current && current.id.startsWith('launcher')) {
              current.close();
            }
            app.shell.activateById(widget.id);
            return;
          }
        }
        const remoteTerminal = new LazyTerminal(system, options, translator);
        remoteTerminal.title.icon = terminalIcon;
        remoteTerminal.id = `${system.toLowerCase()}-terminal-001`;
        remoteTerminal.node.dataset.myCustomId = `${system.toLowerCase()}-terminal-001`;
        remoteTerminal.disposed.connect(() => {
          remoteTerminal.node.dataset.myCustomId = undefined;
          deleteShell(system).catch((err: any) => {
            console.log('Failed to remove job:', err);
          });
        });
        const current = app.shell.currentWidget;
        app.shell.add(remoteTerminal, 'main');
        if (current && current.id.startsWith('launcher')) {
          current.close();
        }
        terminalTracker.add(remoteTerminal);
        await remoteTerminal.shellTermReady;
        if (!remoteTerminal._failed) {
          await remoteTerminal.createLateSession();
        }
      }
    });
    // Add the command to the launcher
    if (launcher) {
      launcher.add({
        command,
        category: PALETTE_CATEGORY,
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
  app.restored.then(() => {
    void (async () => {
      const sessions = await listOpenSessions();
      for (const session of sessions) {
        console.log(`Restore terminal for ${session}`);
        const command = `reverse-unicore-terminal-${session}:create`;
        if (commands.hasCommand(command)) {
          await commands.execute(command);
        } else {
          console.warn(`Command not found: ${command}`);
        }
      }
    })();
  });
}

export default plugin;
