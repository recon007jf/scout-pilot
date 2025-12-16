import React, { useState } from 'react';
import { TerminalLayout } from './components/layout/TerminalLayout';
import { BootSequence } from './components/onboarding/BootSequence';
import { AgencySelection } from './components/onboarding/AgencySelection';

function App() {
  const [booted, setBooted] = useState(false);
  const [agencySelected, setAgencySelected] = useState(false);

  return (
    <TerminalLayout>
      {!booted ? (
        <BootSequence onComplete={() => setBooted(true)} />
      ) : !agencySelected ? (
        <AgencySelection onComplete={() => setAgencySelected(true)} />
      ) : (
        <div className="w-full h-full flex items-center justify-center flex-col gap-4">
          <h1 className="text-4xl md:text-6xl font-bold text-glow tracking-tighter">
            AWAITING HANDLE...
          </h1>
        </div>
      )}
    </TerminalLayout>
  );
}

export default App;
