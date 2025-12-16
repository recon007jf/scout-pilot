import { create } from 'zustand';

interface GameState {
    traceLevel: number;
    helixTrust: number;
    currentMission: string | null;
    agency: 'CIA' | 'NSA' | 'MI6' | null;
    handle: string | null;

    // Actions
    setTraceLevel: (level: number) => void;
    increaseTrace: (amount: number) => void;
    decreaseTrace: (amount: number) => void;
    setHelixTrust: (trust: number) => void;
    setAgency: (agency: 'CIA' | 'NSA' | 'MI6') => void;
    setHandle: (handle: string) => void;
}

export const useGameStore = create<GameState>((set) => ({
    traceLevel: 0,
    helixTrust: 50, // Starts neutral
    currentMission: 'MISSION_00_INIT',
    agency: null,
    handle: null,

    setTraceLevel: (level) => set({ traceLevel: level }),
    increaseTrace: (amount) => set((state) => ({ traceLevel: Math.min(100, state.traceLevel + amount) })),
    decreaseTrace: (amount) => set((state) => ({ traceLevel: Math.max(0, state.traceLevel - amount) })),
    setHelixTrust: (trust) => set({ helixTrust: trust }),
    setAgency: (agency) => set({ agency }),
    setHandle: (handle) => set({ handle }),
}));
