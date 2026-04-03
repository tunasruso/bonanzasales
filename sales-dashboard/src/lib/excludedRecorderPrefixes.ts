const EXCLUDED_RECORDERS_ENDPOINT = '/api/excluded-recorder-prefixes';

export async function fetchExcludedRecorderPrefixes(): Promise<string[]> {
  try {
    const response = await fetch(EXCLUDED_RECORDERS_ENDPOINT);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    if (!Array.isArray(data?.prefixes)) {
      return [];
    }

    return data.prefixes.filter((value: unknown): value is string => typeof value === 'string' && value.length > 0);
  } catch (error) {
    console.error('Error fetching excluded recorder prefixes:', error);
    return [];
  }
}
