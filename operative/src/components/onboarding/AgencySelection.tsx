import React from 'react';
import { motion } from 'framer-motion';
import { useGameStore } from '../../store/gameStore';
import { cn } from '../../lib/utils';

interface AgencySelectionProps {
    onComplete: () => void;
}

const AGENCIES = [
    {
        id: 'CIA',
        name: 'THE COMPANY',
        desc: 'Human Intelligence & Destabilization',
        bonus: 'BONUS: Social Engineering +20%',
        color: 'text-agency-amber',
        border: 'border-agency-amber',
        bg: 'hover:bg-agency-amber/10'
    },
    {
        id: 'NSA',
        name: 'THE FORT',
        desc: 'Signals Intelligence & Surveillance',
        bonus: 'BONUS: Satellite Uplink +20%',
        color: 'text-agency-cyan',
        border: 'border-agency-cyan',
        bg: 'hover:bg-agency-cyan/10'
    },
    {
        id: 'MI6',
        name: 'THE CIRCUS',
        desc: 'Tradecraft & Political Maneuvering',
        bonus: 'BONUS: Subtlety +20%',
        color: 'text-agency-crimson',
        border: 'border-agency-crimson',
        bg: 'hover:bg-agency-crimson/10'
    }
] as const;

export const AgencySelection: React.FC<AgencySelectionProps> = ({ onComplete }) => {
    const setAgency = useGameStore(state => state.setAgency);

    const handleSelect = (agencyId: 'CIA' | 'NSA' | 'MI6') => {
        setAgency(agencyId);
        onComplete();
    };

    return (
        <div className="w-full h-full flex flex-col items-center justify-center p-8">
            <motion.h2
                initial={{ opacity: 0, y: -20 }}
                animate={{ opacity: 1, y: 0 }}
                className="text-2xl md:text-3xl mb-12 tracking-[0.2em] text-glow"
            >
                SELECT AFFILIATION
            </motion.h2>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 w-full max-w-5xl">
                {AGENCIES.map((agency, index) => (
                    <motion.button
                        key={agency.id}
                        initial={{ opacity: 0, y: 20 }}
                        animate={{ opacity: 1, y: 0 }}
                        transition={{ delay: index * 0.2 }}
                        onClick={() => handleSelect(agency.id)}
                        className={cn(
                            "group relative p-6 border border-opacity-30 text-left transition-all duration-300",
                            agency.border,
                            agency.bg
                        )}
                    >
                        <div className={cn("text-4xl font-bold mb-2 opacity-50 group-hover:opacity-100 transition-opacity", agency.color)}>
                            {agency.id}
                        </div>
                        <div className="text-xl font-bold mb-4 tracking-wider">{agency.name}</div>
                        <div className="text-sm opacity-70 mb-4 font-sans">{agency.desc}</div>
                        <div className={cn("text-xs uppercase tracking-widest", agency.color)}>
                            {agency.bonus}
                        </div>

                        {/* Corner Accents */}
                        <div className={cn("absolute top-0 left-0 w-2 h-2 border-t-2 border-l-2", agency.border)}></div>
                        <div className={cn("absolute top-0 right-0 w-2 h-2 border-t-2 border-r-2", agency.border)}></div>
                        <div className={cn("absolute bottom-0 left-0 w-2 h-2 border-b-2 border-l-2", agency.border)}></div>
                        <div className={cn("absolute bottom-0 right-0 w-2 h-2 border-b-2 border-r-2", agency.border)}></div>
                    </motion.button>
                ))}
            </div>
        </div>
    );
};
