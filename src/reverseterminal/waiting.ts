// Copyright (c) Jupyter Development Team.
// Distributed under the terms of the Modified BSD License.

import {
  ITranslator,
  nullTranslator
  // TranslationBundle
} from '@jupyterlab/translation';
import { PromiseDelegate } from '@lumino/coreutils';
import { Platform } from '@lumino/domutils';
import { Message, MessageLoop } from '@lumino/messaging';
import { Widget } from '@lumino/widgets';
import type {
  ITerminalInitOnlyOptions,
  ITerminalOptions,
  Terminal as Xterm
} from '@xterm/xterm';
import type { CanvasAddon } from '@xterm/addon-canvas';
import type { FitAddon } from '@xterm/addon-fit';
import type { WebLinksAddon } from '@xterm/addon-web-links';
import type { WebglAddon } from '@xterm/addon-webgl';
import { ITerminal } from '@jupyterlab/terminal';

import { retrieveShell, IShellStatusEvent } from '../handler';

/**
 * The class name added to a terminal widget.
 */
const TERMINAL_CLASS = 'jp-Terminal';

/**
 * The class name added to a terminal body.
 */
const TERMINAL_BODY_CLASS = 'jp-Terminal-body';

/**
 * A widget which manages a terminal session.
 */
export class WaitingTerminalWidget extends Widget {
  /**
   * Construct a new terminal widget.
   *
   * @param system - The terminal system object.
   *
   * @param options - The terminal configuration options.
   *
   * @param translator - The language translator.
   */

  constructor(
    system: string,
    options: Partial<ITerminal.IOptions> = {},
    translator?: ITranslator
  ) {
    super();
    translator = translator || nullTranslator;
    // this._trans = translator.load('jupyterlab');

    this._failed = false;
    this._system = system;
    this._port = '';
    this._host = '';

    // Initialize settings.
    this._options = { ...ITerminal.defaultOptions, ...options };

    const { theme, ...other } = this._options;
    const xtermOptions = {
      theme: Private.getXTermTheme(theme),
      ...other
    };

    this.addClass(TERMINAL_CLASS);

    this._setThemeAttribute(theme);

    // Create the xterm.
    Private.createTerminal(xtermOptions)
      .then(([term, fitAddon]) => {
        this._term = term;
        this._fitAddon = fitAddon;
        this._initializeTerm();

        this.id = `jp-TerminalWaiting-${Private.id++}`;
        // this.title.label = this._trans.__('Terminal');
        this._isReady = true;
        this._ready.resolve();

        this.update();
        this._term.write(`Setting up your terminal session ${this._system} ...`);

        this._waitForTerminal(this._system);
      })
      .catch(reason => {
        console.error('Failed to create a terminal.\n', reason);
        this._ready.reject(reason);
      });
  }

  private printStatus(data: IShellStatusEvent) {
    if (data.newline !== false) {
      this._term.writeln('');
      this._term.write(data.msg);
    } else {
      this._term.write(data.msg);
    }
    if (data.ready === true) {
      if (data.port) {
        this._port = data.port;
      }
      if (data.host) {
        this._host = data.host;
      }
      this._shellTermReady.resolve();
    } else if (data.failed === true) {
      this._failed = true;
      this._shellTermReady.resolve();
    }
  }

  private async _waitForTerminal(system: string) {
    retrieveShell(system, data => this.printStatus(data));
  }

  /**
   * A promise that is fulfilled when the terminal is ready.
   */
  get ready(): Promise<void> {
    return this._ready.promise;
  }

  get shellTermReady(): Promise<void> {
    return this._shellTermReady.promise;
  }

  /**
   * Get a config option for the terminal.
   */
  getOption<K extends keyof ITerminal.IOptions>(
    option: K
  ): ITerminal.IOptions[K] {
    return this._options[option];
  }

  /**
   * Set a config option for the terminal.
   */
  setOption<K extends keyof ITerminal.IOptions>(
    option: K,
    value: ITerminal.IOptions[K]
  ): void {
    if (
      option !== 'theme' &&
      (this._options[option] === value || option === 'initialCommand')
    ) {
      return;
    }

    this._options[option] = value;

    switch (option) {
      case 'fontFamily':
        this._term.options.fontFamily = value as string | undefined;
        break;
      case 'fontSize':
        this._term.options.fontSize = value as number | undefined;
        break;
      case 'lineHeight':
        this._term.options.lineHeight = value as number | undefined;
        break;
      case 'screenReaderMode':
        this._term.options.screenReaderMode = value as boolean | undefined;
        break;
      case 'scrollback':
        this._term.options.scrollback = value as number | undefined;
        break;
      case 'theme':
        this._term.options.theme = {
          ...Private.getXTermTheme(value as ITerminal.Theme)
        };
        this._setThemeAttribute(value as ITerminal.Theme);
        break;
      case 'macOptionIsMeta':
        this._term.options.macOptionIsMeta = value as boolean | undefined;
        break;
      default:
        // Do not transmit options not listed above to XTerm
        break;
    }

    this._needsResize = true;
    this.update();
  }

  /**
   * Dispose of the resources held by the terminal widget.
   */
  dispose(): void {
    void this.ready.then(() => {
      this._term.dispose();
    });
    super.dispose();
  }

  /**
   * Refresh the terminal session.
   *
   * #### Notes
   * Failure to reconnect to the session should be caught appropriately
   */
  async refresh(): Promise<void> {
    if (!this.isDisposed && this._isReady) {
      this._term.clear();
    }
  }

  /**
   * Check if terminal has any text selected.
   */
  hasSelection(): boolean {
    if (!this.isDisposed && this._isReady) {
      return this._term.hasSelection();
    }
    return false;
  }

  /**
   * Paste text into terminal.
   */
  paste(data: string): void {
    if (!this.isDisposed && this._isReady) {
      return this._term.paste(data);
    }
  }

  /**
   * Get selected text from terminal.
   */
  getSelection(): string | null {
    if (!this.isDisposed && this._isReady) {
      return this._term.getSelection();
    }
    return null;
  }

  /**
   * Process a message sent to the widget.
   *
   * @param msg - The message sent to the widget.
   *
   * #### Notes
   * Subclasses may reimplement this method as needed.
   */
  processMessage(msg: Message): void {
    super.processMessage(msg);
    switch (msg.type) {
      case 'fit-request':
        this.onFitRequest(msg);
        break;
      default:
        break;
    }
  }

  /**
   * Set the size of the terminal when attached if dirty.
   */
  protected onAfterAttach(msg: Message): void {
    this.update();
  }

  /**
   * Set the size of the terminal when shown if dirty.
   */
  protected onAfterShow(msg: Message): void {
    this.update();
  }

  /**
   * On resize, use the computed row and column sizes to resize the terminal.
   */
  protected onResize(msg: Widget.ResizeMessage): void {
    this._offsetWidth = msg.width;
    this._offsetHeight = msg.height;
    this._needsResize = true;
    this.update();
  }

  /**
   * A message handler invoked on an `'update-request'` message.
   */
  protected onUpdateRequest(msg: Message): void {
    if (!this.isVisible || !this.isAttached || !this._isReady) {
      return;
    }

    // Open the terminal if necessary.
    if (!this._termOpened) {
      this._term.open(this.node);
      this._term.element?.classList.add(TERMINAL_BODY_CLASS);
      this._termOpened = true;
    }

    if (this._needsResize) {
      this._resizeTerminal();
    }
  }

  /**
   * A message handler invoked on an `'fit-request'` message.
   */
  protected onFitRequest(msg: Message): void {
    const resize = Widget.ResizeMessage.UnknownSize;
    MessageLoop.sendMessage(this, resize);
  }

  /**
   * Handle `'activate-request'` messages.
   */
  protected onActivateRequest(msg: Message): void {
    this._term?.focus();
  }

  /**
   * Initialize the terminal object.
   */
  private _initializeTerm(): void {
    const term = this._term;
    term.onData((data: string) => {
      if (this.isDisposed) {
        return;
      }
    });

    term.onTitleChange((title: string) => {
      this.title.label = title;
    });

    // Do not add any Ctrl+C/Ctrl+V handling on macOS,
    // where Cmd+C/Cmd+V works as intended.
    if (Platform.IS_MAC) {
      return;
    }

    term.attachCustomKeyEventHandler(event => {
      if (event.ctrlKey && event.key === 'c' && term.hasSelection()) {
        // Return so that the usual OS copy happens
        // instead of interrupt signal.
        return false;
      }

      if (event.ctrlKey && event.key === 'v' && this._options.pasteWithCtrlV) {
        // Return so that the usual paste happens.
        return false;
      }

      return true;
    });
  }

  /**
   * Resize the terminal based on computed geometry.
   */
  private _resizeTerminal() {
    if (this._options.autoFit) {
      this._fitAddon.fit();
    }
    if (this._offsetWidth === -1) {
      this._offsetWidth = this.node.offsetWidth;
    }
    if (this._offsetHeight === -1) {
      this._offsetHeight = this.node.offsetHeight;
    }
    this._needsResize = false;
  }

  private _setThemeAttribute(theme: string | null | undefined) {
    if (this.isDisposed) {
      return;
    }

    this.node.setAttribute(
      'data-term-theme',
      theme ? theme.toLowerCase() : 'inherit'
    );
  }

  private _fitAddon!: FitAddon;
  public _port: string;
  public _host: string;
  public _failed: boolean;
  private _system: string;
  private _needsResize = true;
  private _offsetWidth = -1;
  private _offsetHeight = -1;
  private _options: ITerminal.IOptions;
  private _isReady = false;
  private _ready = new PromiseDelegate<void>();
  private _shellTermReady = new PromiseDelegate<void>();
  private _term!: Xterm;
  private _termOpened = false;
  // private _trans: TranslationBundle;
}

/**
 * A namespace for private data.
 */
namespace Private {
  /**
   * An incrementing counter for ids.
   */
  // eslint-disable-next-line prefer-const
  export let id = 0;

  /**
   * The light terminal theme.
   */
  export const lightTheme: ITerminal.IThemeObject = {
    foreground: '#000',
    background: '#fff',
    cursor: '#616161', // md-grey-700
    cursorAccent: '#F5F5F5', // md-grey-100
    selectionBackground: 'rgba(97, 97, 97, 0.3)', // md-grey-700
    selectionInactiveBackground: 'rgba(189, 189, 189, 0.3)' // md-grey-400
  };

  /**
   * The dark terminal theme.
   */
  export const darkTheme: ITerminal.IThemeObject = {
    foreground: '#fff',
    background: '#000',
    cursor: '#fff',
    cursorAccent: '#000',
    selectionBackground: 'rgba(255, 255, 255, 0.3)',
    selectionInactiveBackground: 'rgba(238, 238, 238, 0.3)' // md-grey-200
  };

  /**
   * The current theme.
   */
  export const inheritTheme = (): ITerminal.IThemeObject => ({
    foreground: getComputedStyle(document.body)
      .getPropertyValue('--jp-ui-font-color0')
      .trim(),
    background: getComputedStyle(document.body)
      .getPropertyValue('--jp-layout-color0')
      .trim(),
    cursor: getComputedStyle(document.body)
      .getPropertyValue('--jp-ui-font-color1')
      .trim(),
    cursorAccent: getComputedStyle(document.body)
      .getPropertyValue('--jp-ui-inverse-font-color0')
      .trim(),
    selectionBackground: getComputedStyle(document.body)
      .getPropertyValue('--jp-layout-color3')
      .trim(),
    selectionInactiveBackground: getComputedStyle(document.body)
      .getPropertyValue('--jp-layout-color2')
      .trim()
  });

  export function getXTermTheme(
    theme: ITerminal.Theme
  ): ITerminal.IThemeObject {
    switch (theme) {
      case 'light':
        return lightTheme;
      case 'dark':
        return darkTheme;
      case 'inherit':
      default:
        return inheritTheme();
    }
  }
}

/**
 * Utility functions for creating a Terminal widget
 */
namespace Private {
  let supportWebGL: boolean = false;
  let Xterm_: typeof Xterm;
  let FitAddon_: typeof FitAddon;
  let WeblinksAddon_: typeof WebLinksAddon;
  let Renderer_: typeof CanvasAddon | typeof WebglAddon;

  /**
   * Detect if the browser supports WebGL or not.
   *
   * Reference: https://developer.mozilla.org/en-US/docs/Web/API/WebGL_API/By_example/Detect_WebGL
   */
  // eslint-disable-next-line no-inner-declarations
  function hasWebGLContext(): boolean {
    // Create canvas element. The canvas is not added to the
    // document itself, so it is never displayed in the
    // browser window.
    const canvas = document.createElement('canvas');

    // Get WebGLRenderingContext from canvas element.
    const gl =
      canvas.getContext('webgl') || canvas.getContext('experimental-webgl');

    // Report the result.
    try {
      return gl instanceof WebGLRenderingContext;
    } catch (error) {
      return false;
    }
  }

  // eslint-disable-next-line no-inner-declarations
  function addRenderer(term: Xterm): void {
    const renderer = new Renderer_();
    term.loadAddon(renderer);
    if (supportWebGL) {
      (renderer as WebglAddon).onContextLoss(event => {
        console.debug('WebGL context lost - reinitialize Xtermjs renderer.');
        renderer.dispose();
        // If the Webgl context is lost, reinitialize the addon
        addRenderer(term);
      });
    }
  }

  /**
   * Create a xterm.js terminal asynchronously.
   */
  export async function createTerminal(
    options: ITerminalOptions & ITerminalInitOnlyOptions
  ): Promise<[Xterm, FitAddon]> {
    if (!Xterm_) {
      supportWebGL = hasWebGLContext();
      const [xterm_, fitAddon_, renderer_, weblinksAddon_] = await Promise.all([
        import('@xterm/xterm'),
        import('@xterm/addon-fit'),
        supportWebGL
          ? import('@xterm/addon-webgl')
          : import('@xterm/addon-canvas'),
        import('@xterm/addon-web-links')
      ]);
      Xterm_ = xterm_.Terminal;
      FitAddon_ = fitAddon_.FitAddon;
      Renderer_ =
        (renderer_ as any).WebglAddon ?? (renderer_ as any).CanvasAddon;
      WeblinksAddon_ = weblinksAddon_.WebLinksAddon;
    }

    const term = new Xterm_(options);
    addRenderer(term);
    const fitAddon = new FitAddon_();
    term.loadAddon(fitAddon);
    term.loadAddon(new WeblinksAddon_());
    return [term, fitAddon];
  }
}
