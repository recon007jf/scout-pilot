import React, { useRef, useMemo } from 'react';
import { Canvas, useFrame } from '@react-three/fiber';
import { OrbitControls, Sphere, Stars } from '@react-three/drei';
import * as THREE from 'three';

const Globe = () => {
    const earthRef = useRef<THREE.Mesh>(null);
    const cloudsRef = useRef<THREE.Mesh>(null);

    useFrame(({ clock }) => {
        if (earthRef.current) {
            earthRef.current.rotation.y = clock.getElapsedTime() * 0.05;
        }
        if (cloudsRef.current) {
            cloudsRef.current.rotation.y = clock.getElapsedTime() * 0.07;
        }
    });

    // Custom Shader for the "Holographic" Atmosphere
    const atmosphereShader = useMemo(() => ({
        uniforms: {
            c: { value: 0.3 },
            p: { value: 5.0 },
            glowColor: { value: new THREE.Color(0x00f0ff) },
            viewVector: { value: new THREE.Vector3() }
        },
        vertexShader: `
      uniform vec3 viewVector;
      varying float intensity;
      void main() {
        vec3 vNormal = normalize(normalMatrix * normal);
        vec3 vNormel = normalize(normalMatrix * viewVector);
        intensity = pow(0.6 - dot(vNormal, vNormel), 4.0);
        gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
      }
    `,
        fragmentShader: `
      uniform vec3 glowColor;
      varying float intensity;
      void main() {
        vec3 glow = glowColor * intensity;
        gl_FragColor = vec4( glow, 1.0 );
      }
    `,
        side: THREE.FrontSide,
        blending: THREE.AdditiveBlending,
        transparent: true
    }), []);

    return (
        <group>
            {/* Core Earth Sphere - Wireframe/Grid */}
            <Sphere ref={earthRef} args={[2, 64, 64]}>
                <meshBasicMaterial
                    color="#002030"
                    wireframe
                    transparent
                    opacity={0.1}
                />
            </Sphere>

            {/* Surface Dots/Grid for "Tech" look */}
            <points>
                <sphereGeometry args={[2.01, 64, 64]} />
                <pointsMaterial
                    color="#00f0ff"
                    size={0.02}
                    transparent
                    opacity={0.4}
                    sizeAttenuation
                />
            </points>

            {/* Atmosphere Glow */}
            <Sphere args={[2.2, 64, 64]}>
                <shaderMaterial
                    attach="material"
                    args={[atmosphereShader]}
                    uniforms-viewVector-value={new THREE.Vector3(0, 0, 5)} // Approximate camera pos
                />
            </Sphere>
        </group>
    );
};

export const GlobeViewer = () => {
    return (
        <div className="w-full h-full relative bg-black">
            {/* UI Overlays */}
            <div className="absolute top-4 left-4 z-10 pointer-events-none">
                <h3 className="text-agency-cyan text-xl font-bold tracking-widest text-glow">GLOBAL_SURVEILLANCE</h3>
                <div className="text-xs text-agency-cyan/60">LAT: 34.0522 N | LON: 118.2437 W</div>
            </div>

            <Canvas camera={{ position: [0, 0, 6], fov: 45 }}>
                <ambientLight intensity={0.5} />
                <pointLight position={[10, 10, 10]} intensity={1} color="#00f0ff" />

                <Stars radius={300} depth={50} count={5000} factor={4} saturation={0} fade speed={1} />

                <Globe />

                <OrbitControls
                    enablePan={false}
                    enableZoom={true}
                    minDistance={3}
                    maxDistance={10}
                    autoRotate={false}
                />
            </Canvas>
        </div>
    );
};
