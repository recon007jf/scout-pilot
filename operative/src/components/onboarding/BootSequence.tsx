import React, { useEffect, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

interface BootSequenceProps {
    onComplete: () => void;
}

const BOOT_LOGS = [
    "BIOS CHECK... OK",
    "LOADING KERNEL... OK",
    "MOUNTING VOLUMES... OK",
    "DECRYPTING FILESYSTEM...",
    "ESTABLISHING SECURE LINK...",
    "HANDSHAKE ACCEPTED.",
    "INITIALIZING HELIX ENGINE...",
    "SYSTEM READY."
];

export const BootSequence: React.FC<BootSequenceProps> = ({ onComplete }) => {
    const [logs, setLogs] = useState<string[]>([]);

    useEffect(() => {
        let delay = 0;
        BOOT_LOGS.forEach((log, index) => {
            delay += Math.random() * 500 + 200;
            setTimeout(() => {
                setLogs(prev => [...prev, log]);
                if (index === BOOT_LOGS.length - 1) {
                    setTimeout(onComplete, 1000);
                }
            }, delay);
        });
    }, [onComplete]);

    return (
        <div className="w-full h-full flex flex-col justify-end pb-12 pl-4 font-mono text-sm md:text-base">
            <AnimatePresence>
                {logs.map((log, i) => (
                    <motion.div
                        key={i}
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        className="mb-1 text-agency-cyan"
                    >
                        <span className="opacity-50 mr-2">[{new Date().toISOString().split('T')[1].slice(0, 8)}]</span>
                        {log}
                    </motion.div>
                ))}
            </AnimatePresence>
            <motion.div
                className="mt-2 h-4 w-3 bg-agency-cyan animate-pulse"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
            />
        </div>
    );
};
