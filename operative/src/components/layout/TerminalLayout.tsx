import React from 'react';
import { cn } from '../../lib/utils';

interface TerminalLayoutProps {
    children: React.ReactNode;
    className?: string;
}

export const TerminalLayout: React.FC<TerminalLayoutProps> = ({ children, className }) => {
    return (
        <div className="relative w-screen h-screen bg-black overflow-hidden font-mono text-agency-cyan selection:bg-agency-cyan selection:text-black">
            {/* CRT Effects Layer */}
            <div className="absolute inset-0 z-50 pointer-events-none">
                <div className="w-full h-full scanline-overlay opacity-20"></div>
                <div className="w-full h-full bg-[radial-gradient(circle_at_center,transparent_50%,rgba(0,0,0,0.4)_100%)]"></div>
                <div className="w-full h-full crt-flicker bg-white opacity-[0.02]"></div>
            </div>

            {/* Main Content Container */}
            <div className={cn("relative z-10 w-full h-full flex flex-col p-4 md:p-8", className)}>
                {/* Top Status Bar */}
                <header className="flex justify-between items-center border-b border-agency-cyan/30 pb-2 mb-4 text-xs md:text-sm uppercase tracking-widest">
                    <div className="flex items-center gap-4">
                        <span className="text-agency-crimson animate-pulse">● LIVE CONNECTION</span>
                        <span className="opacity-50">ENCRYPTION: AES-4096-GCM</span>
                    </div>
                    <div className="flex items-center gap-4">
                        <span className="opacity-50">MEM: 64TB</span>
                        <span>{new Date().toLocaleTimeString()}</span>
                    </div>
                </header>

                {/* Content Area */}
                <main className="flex-1 relative overflow-hidden border border-agency-cyan/10 bg-agency-gunmetal/50 backdrop-blur-sm shadow-[0_0_20px_rgba(0,240,255,0.1)]">
                    {children}
                </main>

                {/* Bottom Status Bar */}
                <footer className="flex justify-between items-center border-t border-agency-cyan/30 pt-2 mt-4 text-[10px] md:text-xs opacity-60">
                    <div>TERMINAL_ID: <span className="text-agency-amber">GHOST_01</span></div>
                    <div>AGENCY_OS v3.0.4</div>
                </footer>
            </div>
        </div>
    );
};
