import { URLExt } from '@jupyterlab/coreutils';

import { ServerConnection } from '@jupyterlab/services';

export interface IShellStatusEvent {
  msg: string;
  ready?: boolean;
  failed?: boolean;
  newline?: boolean;
}

/**
 * Call the API extension
 *
 * @param path Path argument, must be encoded
 * @param init Initial values for the request
 * @returns The response body interpreted as JSON
 */
export async function requestAPI<T>(
  path = '',
  init: RequestInit = {}
): Promise<T> {
  // Make request to Jupyter API
  const settings = ServerConnection.makeSettings();
  const requestUrl = URLExt.join(
    settings.baseUrl,
    'jupyterlabunicoreshell', // API Namespace
    path
  );

  let response: Response;
  try {
    response = await ServerConnection.makeRequest(requestUrl, init, settings);
  } catch (error) {
    throw new ServerConnection.NetworkError(error as any);
  }

  let data: any = await response.text();

  if (data.length > 0) {
    try {
      data = JSON.parse(data);
    } catch (error) {
      console.log('Not a JSON response body.', response);
    }
  }

  if (!response.ok) {
    throw new ServerConnection.ResponseError(response, data.message || data);
  }
  return data;
}

export async function listSystems(): Promise<string[]> {
  let systems: string[] = [];
  try {
    const data = await requestAPI<any>();
    systems = data;
  } catch (reason) {
    console.error(
      `UNICORE ReverseShell: Could not receive Systems.\n${reason}`
    );
    throw new Error(`Failed to fetch systems\n${reason}`);
  }
  return systems;
}

export function retrieveShell(
  system: string,
  onMessage: (message: IShellStatusEvent) => void,
  onError?: (error: any) => void
): EventSource {
  const settings = ServerConnection.makeSettings();
  const requestUrl = URLExt.join(
    settings.baseUrl,
    'jupyterlabunicoreshell', // API Namespace
    system
  );

  const eventSource = new EventSource(requestUrl);

  eventSource.onmessage = (event: MessageEvent) => {
    const data: IShellStatusEvent = JSON.parse(event.data);
    onMessage(data);
    if (data.ready === true) {
      eventSource.close();
    }
  };

  eventSource.onerror = event => {
    console.error(event);
    eventSource.close();
    if (onError) {
      onError(event);
    }
  };

  return eventSource;
}

export async function deleteShell(system: string): Promise<void> {
  try {
    await requestAPI<any>(system, {
      method: 'DELETE'
    });
  } catch (reason) {
    console.error(`UNICORE ReverseShell: Could not delete shell.\n${reason}`);
    throw new Error(`Failed to delete shell\n${reason}`);
  }
}
