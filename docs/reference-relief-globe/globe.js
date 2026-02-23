import * as THREE from "../pentagram-sagmeister-globes/vendor/three.module.js";
import { OrbitControls } from "../pentagram-sagmeister-globes/vendor/OrbitControls.js";

const ASSETS = {
  heightMapsUltra: [
    "./assets/etopo1_height_16384.png",
    "./assets/etopo1_height_8192.png",
    "./assets/etopo1_height_4096.png",
    "../porcelain-topography/assets/earth_topography_4k.jpg",
  ],
  heightMapsHigh: [
    "./assets/etopo1_height_8192.png",
    "./assets/etopo1_height_4096.png",
    "../porcelain-topography/assets/earth_topography_4k.jpg",
  ],
  heightMapsCompatible: [
    "./assets/etopo1_height_4096.png",
    "../porcelain-topography/assets/earth_topography_4k.jpg",
  ],
  countriesHigh: [
    "../data/ne_10m_admin_0_countries.geojson",
    "../data/ne_110m_admin_0_countries.geojson"
  ],
  countriesCompatible: [
    "../data/ne_10m_admin_0_countries.geojson",
    "../data/ne_110m_admin_0_countries.geojson"
  ],
  statesHigh: [
    "../data/ne_10m_admin_1_states_provinces_lines.geojson"
  ],
  statesCompatible: [
    "../data/ne_10m_admin_1_states_provinces_lines.geojson"
  ],
  riversHigh: [
    "../data/ne_10m_rivers_lake_centerlines.geojson",
    "../data/ne_50m_rivers_lake_centerlines.geojson",
    "../data/ne_110m_rivers_lake_centerlines.geojson"
  ],
  riversCompatible: [
    "../data/ne_50m_rivers_lake_centerlines.geojson",
    "../data/ne_110m_rivers_lake_centerlines.geojson"
  ],
  lakesHigh: [
    "../data/ne_10m_lakes.geojson",
    "../data/ne_50m_lakes.geojson",
    "../data/ne_110m_lakes.geojson"
  ],
  lakesCompatible: [
    "../data/ne_50m_lakes.geojson",
    "../data/ne_110m_lakes.geojson"
  ]
};

const RADIUS = 1.2;
const AUTO_SPIN_SPEED = 0.012;
const LONGITUDE_ORIENTATION = -1;
const DEFAULT_TERRAIN_EXAGGERATION = 0.4;
const DEFAULT_LIGHTING_PRESET = "silver-moon";
const TERRAIN_EXAGGERATION_MIN = 0.4;
const TERRAIN_EXAGGERATION_MAX = 2.4;
const BASE_RELIEF = {
  displacementScale: 0.115,
  bumpScale: 0.05,
  normalScale: 1.2
};
const MATERIAL_DEFAULTS = {
  roughness: 0.82,
  clearcoat: 0.03,
  clearcoatRoughness: 0.9,
  envMapIntensity: 0.14,
  ior: 1.48,
  reflectivity: 0.58,
  specularIntensity: 0.78,
  sheen: 0.045,
  sheenRoughness: 0.84,
  sheenColor: 0xe8edf5,
  normalStrength: 1,
  shellIntensity: 0.03
};
const LIGHTING_DEFAULTS = {
  exposure: 1.04,
  ambientIntensity: 0.55,
  hemiIntensity: 0.22,
  hemiSkyTemp: 7600,
  hemiGroundTemp: 3400,
  frontIntensity: 0.38,
  frontTemp: 6000,
  frontDistance: 4.4,
  keyIntensity: 1.18,
  keyTemp: 5200,
  keyAzimuth: 0,
  keyElevation: 22,
  keyDistance: 5.2,
  fillIntensity: 0.42,
  fillTemp: 7600,
  fillAzimuth: -60,
  fillElevation: 12,
  fillDistance: 6.2,
  rimIntensity: 0.26,
  rimTemp: 9000,
  rimAzimuth: 135,
  rimElevation: -20,
  rimDistance: 6.8,
  accentIntensity: 0.22,
  accentTemp: 3600,
  accentAzimuth: 172,
  accentElevation: 6,
  accentDistance: 8.2
};
const LIGHTING_PRESETS = {
  studio: {
    exposure: 1.04,
    ambientIntensity: 0.55,
    hemiIntensity: 0.22,
    hemiSkyTemp: 7600,
    hemiGroundTemp: 3400,
    frontIntensity: 0.38,
    frontTemp: 6000,
    keyIntensity: 1.18,
    keyTemp: 5200,
    keyAzimuth: 0,
    keyElevation: 22,
    keyDistance: 5.2,
    fillIntensity: 0.42,
    fillTemp: 7600,
    fillAzimuth: -60,
    fillElevation: 12,
    fillDistance: 6.2,
    rimIntensity: 0.26,
    rimTemp: 9000,
    rimAzimuth: 135,
    rimElevation: -20,
    rimDistance: 6.8,
    accentIntensity: 0.22,
    accentTemp: 3600,
    accentAzimuth: 172,
    accentElevation: 6,
    accentDistance: 8.2,
    roughness: 0.82,
    clearcoat: 0.03,
    clearcoatRoughness: 0.9,
    envMapIntensity: 0.14,
    normalStrength: 1,
    shellIntensity: 0.03
  },
  "golden-hour": {
    exposure: 1.16,
    ambientIntensity: 0.38,
    hemiIntensity: 0.28,
    hemiSkyTemp: 5400,
    hemiGroundTemp: 2900,
    frontIntensity: 0.28,
    frontTemp: 5000,
    keyIntensity: 1.65,
    keyTemp: 3850,
    keyAzimuth: -24,
    keyElevation: 20,
    keyDistance: 4.7,
    fillIntensity: 0.3,
    fillTemp: 7100,
    fillAzimuth: 72,
    fillElevation: 7,
    fillDistance: 6.5,
    rimIntensity: 0.48,
    rimTemp: 9800,
    rimAzimuth: 146,
    rimElevation: -6,
    rimDistance: 7.2,
    accentIntensity: 0.34,
    accentTemp: 3150,
    accentAzimuth: -162,
    accentElevation: 16,
    accentDistance: 8.6,
    roughness: 0.74,
    clearcoat: 0.06,
    clearcoatRoughness: 0.86,
    envMapIntensity: 0.2,
    normalStrength: 1.1,
    shellIntensity: 0.05
  },
  dramatic: {
    exposure: 1.22,
    ambientIntensity: 0.24,
    hemiIntensity: 0.12,
    hemiSkyTemp: 6900,
    hemiGroundTemp: 3100,
    frontIntensity: 0.2,
    frontTemp: 5600,
    keyIntensity: 2.2,
    keyTemp: 4700,
    keyAzimuth: 34,
    keyElevation: 18,
    keyDistance: 4.2,
    fillIntensity: 0.12,
    fillTemp: 8200,
    fillAzimuth: -98,
    fillElevation: -8,
    fillDistance: 7.2,
    rimIntensity: 0.72,
    rimTemp: 10400,
    rimAzimuth: 158,
    rimElevation: -14,
    rimDistance: 7.8,
    accentIntensity: 0.44,
    accentTemp: 3250,
    accentAzimuth: -148,
    accentElevation: 22,
    accentDistance: 8.8,
    roughness: 0.68,
    clearcoat: 0.09,
    clearcoatRoughness: 0.72,
    envMapIntensity: 0.28,
    normalStrength: 1.26,
    shellIntensity: 0.08
  },
  arctic: {
    exposure: 1.02,
    ambientIntensity: 0.47,
    hemiIntensity: 0.35,
    hemiSkyTemp: 9800,
    hemiGroundTemp: 5200,
    frontIntensity: 0.34,
    frontTemp: 7800,
    keyIntensity: 1.2,
    keyTemp: 7600,
    keyAzimuth: 8,
    keyElevation: 30,
    keyDistance: 5.4,
    fillIntensity: 0.52,
    fillTemp: 9800,
    fillAzimuth: -48,
    fillElevation: 16,
    fillDistance: 6.4,
    rimIntensity: 0.31,
    rimTemp: 11400,
    rimAzimuth: 124,
    rimElevation: -16,
    rimDistance: 7.1,
    accentIntensity: 0.18,
    accentTemp: 6900,
    accentAzimuth: 170,
    accentElevation: 10,
    accentDistance: 8.3,
    roughness: 0.84,
    clearcoat: 0.04,
    clearcoatRoughness: 0.95,
    envMapIntensity: 0.13,
    normalStrength: 0.95,
    shellIntensity: 0.06
  },
  noir: {
    exposure: 1.35,
    ambientIntensity: 0.18,
    hemiIntensity: 0.08,
    hemiSkyTemp: 6200,
    hemiGroundTemp: 2800,
    frontIntensity: 0.14,
    frontTemp: 5600,
    keyIntensity: 2.4,
    keyTemp: 4400,
    keyAzimuth: 46,
    keyElevation: 10,
    keyDistance: 4.1,
    fillIntensity: 0.07,
    fillTemp: 7600,
    fillAzimuth: -120,
    fillElevation: -14,
    fillDistance: 7.7,
    rimIntensity: 0.92,
    rimTemp: 9600,
    rimAzimuth: 160,
    rimElevation: -18,
    rimDistance: 8.2,
    accentIntensity: 0.52,
    accentTemp: 2950,
    accentAzimuth: -138,
    accentElevation: 14,
    accentDistance: 8.9,
    roughness: 0.63,
    clearcoat: 0.11,
    clearcoatRoughness: 0.62,
    envMapIntensity: 0.34,
    normalStrength: 1.24,
    shellIntensity: 0.02
  },
  "silver-moon": {
    exposure: 1.18,
    ambientIntensity: 0.16,
    hemiIntensity: 0.2,
    hemiSkyTemp: 11000,
    hemiGroundTemp: 2600,
    frontIntensity: 0.18,
    frontTemp: 7600,
    keyIntensity: 1.55,
    keyTemp: 7800,
    keyAzimuth: 28,
    keyElevation: 16,
    keyDistance: 4.6,
    fillIntensity: 0.14,
    fillTemp: 9500,
    fillAzimuth: -95,
    fillElevation: -8,
    fillDistance: 7.4,
    rimIntensity: 0.86,
    rimTemp: 12000,
    rimAzimuth: 152,
    rimElevation: -20,
    rimDistance: 8.2,
    accentIntensity: 0.26,
    accentTemp: 6600,
    accentAzimuth: -160,
    accentElevation: 18,
    accentDistance: 8.7,
    roughness: 0.66,
    clearcoat: 0.11,
    clearcoatRoughness: 0.58,
    envMapIntensity: 0.3,
    normalStrength: 1.12,
    shellIntensity: 0.06
  },
  "museum-soft": {
    exposure: 1.02,
    ambientIntensity: 0.64,
    hemiIntensity: 0.42,
    hemiSkyTemp: 6800,
    hemiGroundTemp: 3600,
    frontIntensity: 0.52,
    frontTemp: 5800,
    keyIntensity: 1.0,
    keyTemp: 5000,
    keyAzimuth: 8,
    keyElevation: 26,
    keyDistance: 5.8,
    fillIntensity: 0.66,
    fillTemp: 7000,
    fillAzimuth: -42,
    fillElevation: 16,
    fillDistance: 6.8,
    rimIntensity: 0.18,
    rimTemp: 7800,
    rimAzimuth: 136,
    rimElevation: -12,
    rimDistance: 7.0,
    accentIntensity: 0.12,
    accentTemp: 4300,
    accentAzimuth: 162,
    accentElevation: 9,
    accentDistance: 8.4,
    roughness: 0.87,
    clearcoat: 0.03,
    clearcoatRoughness: 0.92,
    envMapIntensity: 0.11,
    normalStrength: 0.96,
    shellIntensity: 0.04
  },
  "editorial-split": {
    exposure: 1.14,
    ambientIntensity: 0.28,
    hemiIntensity: 0.16,
    hemiSkyTemp: 7600,
    hemiGroundTemp: 3200,
    frontIntensity: 0.24,
    frontTemp: 6200,
    keyIntensity: 1.9,
    keyTemp: 4500,
    keyAzimuth: -36,
    keyElevation: 24,
    keyDistance: 4.3,
    fillIntensity: 0.16,
    fillTemp: 9000,
    fillAzimuth: 84,
    fillElevation: 2,
    fillDistance: 7.0,
    rimIntensity: 0.58,
    rimTemp: 9800,
    rimAzimuth: 150,
    rimElevation: -10,
    rimDistance: 7.5,
    accentIntensity: 0.34,
    accentTemp: 3400,
    accentAzimuth: -170,
    accentElevation: 4,
    accentDistance: 8.5,
    roughness: 0.7,
    clearcoat: 0.08,
    clearcoatRoughness: 0.68,
    envMapIntensity: 0.26,
    normalStrength: 1.18,
    shellIntensity: 0.05
  },
  "sunset-rimfire": {
    exposure: 1.21,
    ambientIntensity: 0.32,
    hemiIntensity: 0.22,
    hemiSkyTemp: 5600,
    hemiGroundTemp: 2600,
    frontIntensity: 0.2,
    frontTemp: 5200,
    keyIntensity: 1.82,
    keyTemp: 3200,
    keyAzimuth: -18,
    keyElevation: 14,
    keyDistance: 4.4,
    fillIntensity: 0.22,
    fillTemp: 7400,
    fillAzimuth: 74,
    fillElevation: 4,
    fillDistance: 6.9,
    rimIntensity: 0.74,
    rimTemp: 10800,
    rimAzimuth: 160,
    rimElevation: -12,
    rimDistance: 7.9,
    accentIntensity: 0.48,
    accentTemp: 2900,
    accentAzimuth: -150,
    accentElevation: 22,
    accentDistance: 8.8,
    roughness: 0.65,
    clearcoat: 0.1,
    clearcoatRoughness: 0.6,
    envMapIntensity: 0.32,
    normalStrength: 1.24,
    shellIntensity: 0.07
  },
  "high-noon-crisp": {
    exposure: 0.98,
    ambientIntensity: 0.46,
    hemiIntensity: 0.26,
    hemiSkyTemp: 6900,
    hemiGroundTemp: 4000,
    frontIntensity: 0.36,
    frontTemp: 6100,
    keyIntensity: 1.48,
    keyTemp: 5600,
    keyAzimuth: 0,
    keyElevation: 52,
    keyDistance: 5.1,
    fillIntensity: 0.3,
    fillTemp: 7000,
    fillAzimuth: -75,
    fillElevation: 20,
    fillDistance: 6.4,
    rimIntensity: 0.22,
    rimTemp: 8600,
    rimAzimuth: 120,
    rimElevation: -24,
    rimDistance: 7.2,
    accentIntensity: 0.2,
    accentTemp: 5000,
    accentAzimuth: 178,
    accentElevation: 8,
    accentDistance: 8.0,
    roughness: 0.78,
    clearcoat: 0.05,
    clearcoatRoughness: 0.78,
    envMapIntensity: 0.19,
    normalStrength: 1.08,
    shellIntensity: 0.03
  }
};

const stage = document.querySelector("#stage");
const statusEl = document.querySelector("#status");
const terrainInputEl = document.querySelector("#terrain-exaggeration");
const terrainValueEl = document.querySelector("#terrain-exaggeration-value");
const lightingReadoutEl = document.querySelector("#lighting-readout");
const lightingPresetEl = document.querySelector("#lighting-preset");
const lightingPresetValueEl = document.querySelector("#lighting-preset-value");
const lightingResetEl = document.querySelector("#lighting-reset");
const REQUESTED_LIGHTING_PRESET = resolveLightingPresetFromQuery();
const REQUESTED_TONE_MAPPING = resolveToneMappingFromQuery();

if (!stage || !statusEl) {
  throw new Error("Missing required DOM nodes.");
}

boot().catch((error) => {
  console.error(error);
  const message = error && error.message ? error.message : "Unknown initialization error";
  setStatus(`Failed to initialize globe: ${message}`, "warn");
});

async function boot() {
  setStatus("Loading high-resolution elevation and map data...", "loading");

  const browser = detectBrowserProfile();
  const isCompact = window.matchMedia("(max-width: 900px)").matches;
  const profile = selectRenderProfile({
    browser,
    isCompact,
    memoryHint: Number(navigator.deviceMemory || NaN)
  });

  const renderer = createRendererWithFallback(browser);
  if ("outputColorSpace" in renderer) {
    renderer.outputColorSpace = THREE.SRGBColorSpace;
  } else if ("outputEncoding" in renderer && "sRGBEncoding" in THREE) {
    renderer.outputEncoding = THREE.sRGBEncoding;
  }
  renderer.toneMapping = REQUESTED_TONE_MAPPING;
  renderer.toneMappingExposure = LIGHTING_DEFAULTS.exposure;
  renderer.setClearColor(0x000000, 0);
  renderer.domElement.classList.add("globe-canvas");
  stage.appendChild(renderer.domElement);
  const maxTextureSize = Number(renderer?.capabilities?.maxTextureSize || 8192);
  const requestedTextureWidth = resolveSupportedTextureWidth(profile.textureWidth, maxTextureSize);
  const heightMapSources = resolveHeightMapSources(requestedTextureWidth);

  const scene = new THREE.Scene();
  scene.environment = createStudioEnvironmentSafe(renderer);

  const camera = new THREE.PerspectiveCamera(31, 1, 0.05, 80);
  camera.position.set(0, 0, 4.15);

  const controls = new OrbitControls(camera, renderer.domElement);
  controls.enablePan = false;
  controls.enableDamping = true;
  controls.dampingFactor = 0.055;
  controls.rotateSpeed = 0.5;
  controls.zoomSpeed = 0.62;
  controls.minDistance = 1.9;
  controls.maxDistance = 8;
  controls.target.set(0, 0, 0);

  const globeGroup = new THREE.Group();
  globeGroup.rotation.y = 3.35;
  globeGroup.rotation.x = 0;
  scene.add(globeGroup);

  const lightingRig = createLightingRig(scene, camera, controls);
  let globeMaterialRef = null;
  let shellMaterialRef = null;
  const materialSettings = {
    terrainExaggeration: clamp(
      Number(terrainInputEl?.value || DEFAULT_TERRAIN_EXAGGERATION),
      TERRAIN_EXAGGERATION_MIN,
      TERRAIN_EXAGGERATION_MAX
    ),
    normalStrength: MATERIAL_DEFAULTS.normalStrength,
    roughness: MATERIAL_DEFAULTS.roughness,
    clearcoat: MATERIAL_DEFAULTS.clearcoat,
    clearcoatRoughness: MATERIAL_DEFAULTS.clearcoatRoughness,
    envMapIntensity: MATERIAL_DEFAULTS.envMapIntensity,
    ior: MATERIAL_DEFAULTS.ior,
    reflectivity: MATERIAL_DEFAULTS.reflectivity,
    specularIntensity: MATERIAL_DEFAULTS.specularIntensity,
    sheen: MATERIAL_DEFAULTS.sheen,
    sheenRoughness: MATERIAL_DEFAULTS.sheenRoughness,
    sheenColor: MATERIAL_DEFAULTS.sheenColor,
    shellIntensity: MATERIAL_DEFAULTS.shellIntensity
  };
  const lightingControls = setupLightingControls({
    rig: lightingRig,
    renderer,
    materialSettings,
    getGlobeMaterial: () => globeMaterialRef,
    getShellMaterial: () => shellMaterialRef
  });
  setupPresetButtons(lightingControls);

  let freezeAutoSpinUntil = 0;
  controls.addEventListener("start", () => {
    freezeAutoSpinUntil = performance.now() + 2300;
  });

  const resize = () => {
    const width = Math.max(16, stage.clientWidth);
    const height = Math.max(16, stage.clientHeight);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, profile.maxPixelRatio));
    renderer.setSize(width, height, false);
    camera.aspect = width / height;
    camera.updateProjectionMatrix();
  };
  resize();
  window.addEventListener("resize", resize);
  if ("ResizeObserver" in window) {
    new ResizeObserver(resize).observe(stage);
  }

  try {
    setStatus(
      `Loading elevation texture (${Math.round(requestedTextureWidth / 1024)}K target)...`,
      "loading"
    );
    const heightTexture = await loadTextureWithFallback(heightMapSources, {
      timeoutMs: 26000
    });
    const imageSize = getImageIntrinsicSize(heightTexture.image);
    const sourceTextureWidth = resolveSupportedTextureWidth(
      imageSize.width || requestedTextureWidth,
      maxTextureSize
    );
    const textureWidth = Math.min(requestedTextureWidth, sourceTextureWidth);
    const textureHeight = Math.max(2, Math.floor(textureWidth / 2));
    const segments = resolveMeshSegments(textureWidth);

    setStatus("Loading country and state outlines...", "loading");
    const [countriesGeo, stateLinesGeo, riversGeo, lakesGeo] = await Promise.all([
      fetchJsonWithFallback(profile.countriesSources, { timeoutMs: profile.geoTimeoutMs }),
      fetchJsonWithFallback(profile.statesSources, { timeoutMs: profile.stateTimeoutMs }),
      fetchJsonWithFallback(profile.riversSources, { timeoutMs: profile.geoTimeoutMs }),
      fetchJsonWithFallback(profile.lakesSources, { timeoutMs: profile.geoTimeoutMs })
    ]);

    setStatus("Building vector terrain mesh (this may take a few seconds)...", "loading");
    await waitForNextFrame();

    const landMaskCanvas = buildLandMaskCanvas(countriesGeo, textureWidth, textureHeight);
    const landMaskCtx = landMaskCanvas.getContext("2d", { willReadFrequently: true });
    let landMaskData = null;
    if (landMaskCtx) {
      try {
        const landMaskRgba = landMaskCtx.getImageData(0, 0, textureWidth, textureHeight).data;
        landMaskData = extractRedChannel(landMaskRgba, textureWidth, textureHeight);
      } catch (error) {
        console.warn("Unable to read land mask texture data.", error);
      }
    }
    const lakeMaskCanvas = buildLakeMaskCanvas(lakesGeo, textureWidth, textureHeight);
    const lakeMaskCtx = lakeMaskCanvas.getContext("2d", { willReadFrequently: true });
    let lakeMaskData = null;
    if (lakeMaskCtx) {
      try {
        const lakeMaskRgba = lakeMaskCtx.getImageData(0, 0, textureWidth, textureHeight).data;
        lakeMaskData = extractRedChannel(lakeMaskRgba, textureWidth, textureHeight);
      } catch (error) {
        console.warn("Unable to read lake mask texture data.", error);
      }
    }
    const heightSampler = createHeightSampler(heightTexture.image, textureWidth, textureHeight);
    if (typeof heightTexture.dispose === "function") {
      heightTexture.dispose();
    }
    landMaskCanvas.width = 1;
    landMaskCanvas.height = 1;
    lakeMaskCanvas.width = 1;
    lakeMaskCanvas.height = 1;

    const reliefMesh = buildReliefMesh({
      radius: RADIUS,
      widthSegments: segments.width,
      heightSegments: segments.height,
      heightSampler,
      landMaskData,
      landMaskWidth: textureWidth,
      landMaskHeight: textureHeight,
      lakeMaskData,
      lakeMaskWidth: textureWidth,
      lakeMaskHeight: textureHeight,
      terrainExaggeration: materialSettings.terrainExaggeration,
      normalStrength: materialSettings.normalStrength
    });
    const globe = reliefMesh.mesh;
    const globeMaterial = reliefMesh.material;
    globeGroup.add(globe);

    setStatus("Tracing vector borders...", "loading");
    await waitForNextFrame();
    const borderOverlay = buildVectorBorderGroup({
      countriesGeo,
      stateLinesGeo,
      riversGeo,
      lakesGeo,
      radius: RADIUS,
      heightSampler,
      landMaskData,
      landMaskWidth: textureWidth,
      landMaskHeight: textureHeight,
      terrainExaggeration: materialSettings.terrainExaggeration,
      normalStrength: materialSettings.normalStrength,
      countryStepDeg: profile.borderCountryStepDeg,
      stateStepDeg: profile.borderStateStepDeg,
      riverStepDeg: profile.hydroRiverStepDeg,
      lakeStepDeg: profile.hydroLakeStepDeg
    });
    if (borderOverlay.group) {
      globeGroup.add(borderOverlay.group);
    }

    const sheenShell = new THREE.Mesh(
      new THREE.SphereGeometry(RADIUS + 0.006, segments.shellWidth, segments.shellHeight),
      new THREE.MeshBasicMaterial({
        color: 0xffffff,
        transparent: true,
        opacity: MATERIAL_DEFAULTS.shellIntensity,
        side: THREE.BackSide,
        depthWrite: false
      })
    );
    globeGroup.add(sheenShell);
    globeMaterialRef = globeMaterial;
    shellMaterialRef = sheenShell.material;

    const applyTerrain = (factor, normalStrength = materialSettings.normalStrength) => {
      reliefMesh.updateTerrain(factor, normalStrength);
      if (borderOverlay.updateTerrain) {
        borderOverlay.updateTerrain(factor, normalStrength);
      }
      globeMaterial.userData.reliefState = {
        terrainExaggeration: factor,
        normalStrength
      };
    };
    globeMaterial.userData.updateRelief = applyTerrain;

    setupTerrainExaggerationControl({
      materialSettings,
      applyTerrain,
      onChange: lightingControls.refresh
    });
    lightingControls.refresh();

    setStatus("Ready", "ok");
  } catch (error) {
    console.error("Globe generation failed", error);
    setStatus("Failed to load required globe assets", "warn");
  }

  let previous = performance.now();
  const render = (now) => {
    const dt = Math.min(0.05, (now - previous) * 0.001);
    previous = now;

    if (now > freezeAutoSpinUntil) {
      globeGroup.rotation.y += dt * AUTO_SPIN_SPEED;
    }

    controls.update();
    lightingRig.update();
    renderer.render(scene, camera);
    requestAnimationFrame(render);
  };
  requestAnimationFrame(render);
}

function setStatus(text, state) {
  statusEl.textContent = text;
  if (state) {
    statusEl.dataset.state = state;
  } else {
    delete statusEl.dataset.state;
  }
}

function setupTerrainExaggerationControl({ materialSettings, applyTerrain, onChange }) {
  if (!materialSettings || typeof applyTerrain !== "function") {
    return;
  }

  let pendingFactor = clamp(
    Number(materialSettings.terrainExaggeration) || 1,
    TERRAIN_EXAGGERATION_MIN,
    TERRAIN_EXAGGERATION_MAX
  );
  let rafId = 0;

  const updateLabel = (factor) => {
    if (terrainValueEl) {
      terrainValueEl.value = `${factor.toFixed(2)}x`;
      terrainValueEl.textContent = `${factor.toFixed(2)}x`;
    }
  };

  const flush = () => {
    rafId = 0;
    materialSettings.terrainExaggeration = pendingFactor;
    applyTerrain(pendingFactor, materialSettings.normalStrength);
    updateLabel(pendingFactor);
    if (typeof onChange === "function") {
      onChange();
    }
  };

  const schedule = (rawValue) => {
    pendingFactor = clamp(Number(rawValue) || 1, TERRAIN_EXAGGERATION_MIN, TERRAIN_EXAGGERATION_MAX);
    if (!rafId) {
      rafId = requestAnimationFrame(flush);
    }
  };

  schedule(materialSettings.terrainExaggeration);

  if (terrainInputEl) {
    terrainInputEl.addEventListener("input", (event) => {
      const target = event.target;
      schedule(target ? target.value : 1);
    });
  }
}

function setupLightingControls({ rig, renderer, materialSettings, getGlobeMaterial, getShellMaterial }) {
  if (!rig || !renderer || !materialSettings) {
    return { refresh: () => {} };
  }

  const controls = [
    { id: "light-exposure", output: "light-exposure-value", fmt: formatNumber2, get: () => rig.settings.exposure, set: (value) => { rig.settings.exposure = value; } },
    { id: "light-ambient-intensity", output: "light-ambient-intensity-value", fmt: formatNumber2, get: () => rig.settings.ambientIntensity, set: (value) => { rig.settings.ambientIntensity = value; } },
    { id: "light-hemi-intensity", output: "light-hemi-intensity-value", fmt: formatNumber2, get: () => rig.settings.hemiIntensity, set: (value) => { rig.settings.hemiIntensity = value; } },
    { id: "light-hemi-sky-temp", output: "light-hemi-sky-temp-value", fmt: formatKelvin, get: () => rig.settings.hemiSkyTemp, set: (value) => { rig.settings.hemiSkyTemp = value; } },
    { id: "light-hemi-ground-temp", output: "light-hemi-ground-temp-value", fmt: formatKelvin, get: () => rig.settings.hemiGroundTemp, set: (value) => { rig.settings.hemiGroundTemp = value; } },
    { id: "light-front-intensity", output: "light-front-intensity-value", fmt: formatNumber2, get: () => rig.settings.frontIntensity, set: (value) => { rig.settings.frontIntensity = value; } },
    { id: "light-front-temp", output: "light-front-temp-value", fmt: formatKelvin, get: () => rig.settings.frontTemp, set: (value) => { rig.settings.frontTemp = value; } },
    { id: "light-key-intensity", output: "light-key-intensity-value", fmt: formatNumber2, get: () => rig.settings.keyIntensity, set: (value) => { rig.settings.keyIntensity = value; } },
    { id: "light-key-temp", output: "light-key-temp-value", fmt: formatKelvin, get: () => rig.settings.keyTemp, set: (value) => { rig.settings.keyTemp = value; } },
    { id: "light-key-azimuth", output: "light-key-azimuth-value", fmt: formatDegrees, get: () => rig.settings.keyAzimuth, set: (value) => { rig.settings.keyAzimuth = value; } },
    { id: "light-key-elevation", output: "light-key-elevation-value", fmt: formatDegrees, get: () => rig.settings.keyElevation, set: (value) => { rig.settings.keyElevation = value; } },
    { id: "light-key-distance", output: "light-key-distance-value", fmt: formatNumber2, get: () => rig.settings.keyDistance, set: (value) => { rig.settings.keyDistance = value; } },
    { id: "light-fill-intensity", output: "light-fill-intensity-value", fmt: formatNumber2, get: () => rig.settings.fillIntensity, set: (value) => { rig.settings.fillIntensity = value; } },
    { id: "light-fill-temp", output: "light-fill-temp-value", fmt: formatKelvin, get: () => rig.settings.fillTemp, set: (value) => { rig.settings.fillTemp = value; } },
    { id: "light-fill-azimuth", output: "light-fill-azimuth-value", fmt: formatDegrees, get: () => rig.settings.fillAzimuth, set: (value) => { rig.settings.fillAzimuth = value; } },
    { id: "light-fill-elevation", output: "light-fill-elevation-value", fmt: formatDegrees, get: () => rig.settings.fillElevation, set: (value) => { rig.settings.fillElevation = value; } },
    { id: "light-fill-distance", output: "light-fill-distance-value", fmt: formatNumber2, get: () => rig.settings.fillDistance, set: (value) => { rig.settings.fillDistance = value; } },
    { id: "light-rim-intensity", output: "light-rim-intensity-value", fmt: formatNumber2, get: () => rig.settings.rimIntensity, set: (value) => { rig.settings.rimIntensity = value; } },
    { id: "light-rim-temp", output: "light-rim-temp-value", fmt: formatKelvin, get: () => rig.settings.rimTemp, set: (value) => { rig.settings.rimTemp = value; } },
    { id: "light-rim-azimuth", output: "light-rim-azimuth-value", fmt: formatDegrees, get: () => rig.settings.rimAzimuth, set: (value) => { rig.settings.rimAzimuth = value; } },
    { id: "light-rim-elevation", output: "light-rim-elevation-value", fmt: formatDegrees, get: () => rig.settings.rimElevation, set: (value) => { rig.settings.rimElevation = value; } },
    { id: "light-rim-distance", output: "light-rim-distance-value", fmt: formatNumber2, get: () => rig.settings.rimDistance, set: (value) => { rig.settings.rimDistance = value; } },
    { id: "light-accent-intensity", output: "light-accent-intensity-value", fmt: formatNumber2, get: () => rig.settings.accentIntensity, set: (value) => { rig.settings.accentIntensity = value; } },
    { id: "light-accent-temp", output: "light-accent-temp-value", fmt: formatKelvin, get: () => rig.settings.accentTemp, set: (value) => { rig.settings.accentTemp = value; } },
    { id: "light-accent-azimuth", output: "light-accent-azimuth-value", fmt: formatDegrees, get: () => rig.settings.accentAzimuth, set: (value) => { rig.settings.accentAzimuth = value; } },
    { id: "light-accent-elevation", output: "light-accent-elevation-value", fmt: formatDegrees, get: () => rig.settings.accentElevation, set: (value) => { rig.settings.accentElevation = value; } },
    { id: "light-accent-distance", output: "light-accent-distance-value", fmt: formatNumber2, get: () => rig.settings.accentDistance, set: (value) => { rig.settings.accentDistance = value; } },
    { id: "surface-roughness", output: "surface-roughness-value", fmt: formatNumber2, get: () => materialSettings.roughness, set: (value) => { materialSettings.roughness = value; } },
    { id: "surface-clearcoat", output: "surface-clearcoat-value", fmt: formatNumber2, get: () => materialSettings.clearcoat, set: (value) => { materialSettings.clearcoat = value; } },
    { id: "surface-clearcoat-roughness", output: "surface-clearcoat-roughness-value", fmt: formatNumber2, get: () => materialSettings.clearcoatRoughness, set: (value) => { materialSettings.clearcoatRoughness = value; } },
    { id: "surface-env-intensity", output: "surface-env-intensity-value", fmt: formatNumber2, get: () => materialSettings.envMapIntensity, set: (value) => { materialSettings.envMapIntensity = value; } },
    { id: "surface-normal-strength", output: "surface-normal-strength-value", fmt: formatNumber2, get: () => materialSettings.normalStrength, set: (value) => { materialSettings.normalStrength = value; } },
    { id: "surface-shell-intensity", output: "surface-shell-intensity-value", fmt: formatNumber2, get: () => materialSettings.shellIntensity, set: (value) => { materialSettings.shellIntensity = value; } }
  ];

  const bindings = [];
  let activePreset = DEFAULT_LIGHTING_PRESET;

  const updatePresetLabel = (label) => {
    if (lightingPresetValueEl) {
      lightingPresetValueEl.textContent = label;
    }
  };

  const syncAllControls = () => {
    for (let i = 0; i < bindings.length; i += 1) {
      const binding = bindings[i];
      const value = binding.def.get();
      binding.input.value = String(value);
      if (binding.output) {
        binding.output.textContent = binding.def.fmt(value);
      }
    }
  };

  const updateReadout = () => {
    if (!lightingReadoutEl) {
      return;
    }
    const s = rig.settings;
    const m = materialSettings;
    const presetLabel = lightingPresetValueEl?.textContent?.trim() || "Custom";
    lightingReadoutEl.textContent =
      `preset=${presetLabel} exp=${s.exposure.toFixed(2)} terrain=${m.terrainExaggeration.toFixed(2)}x\n` +
      `ambient=${s.ambientIntensity.toFixed(2)} hemi=${s.hemiIntensity.toFixed(2)} sky=${Math.round(s.hemiSkyTemp)}K ground=${Math.round(s.hemiGroundTemp)}K\n` +
      `camFill i=${s.frontIntensity.toFixed(2)} t=${Math.round(s.frontTemp)}K\n` +
      `key    i=${s.keyIntensity.toFixed(2)} t=${Math.round(s.keyTemp)}K az=${Math.round(s.keyAzimuth)}deg el=${Math.round(s.keyElevation)}deg d=${s.keyDistance.toFixed(2)}\n` +
      `fill   i=${s.fillIntensity.toFixed(2)} t=${Math.round(s.fillTemp)}K az=${Math.round(s.fillAzimuth)}deg el=${Math.round(s.fillElevation)}deg d=${s.fillDistance.toFixed(2)}\n` +
      `rim    i=${s.rimIntensity.toFixed(2)} t=${Math.round(s.rimTemp)}K az=${Math.round(s.rimAzimuth)}deg el=${Math.round(s.rimElevation)}deg d=${s.rimDistance.toFixed(2)}\n` +
      `accent i=${s.accentIntensity.toFixed(2)} t=${Math.round(s.accentTemp)}K az=${Math.round(s.accentAzimuth)}deg el=${Math.round(s.accentElevation)}deg d=${s.accentDistance.toFixed(2)}\n` +
      `surface rough=${m.roughness.toFixed(2)} clear=${m.clearcoat.toFixed(2)} clrR=${m.clearcoatRoughness.toFixed(2)} refl=${m.envMapIntensity.toFixed(2)} relief=${m.normalStrength.toFixed(2)} atmo=${m.shellIntensity.toFixed(2)}`;
  };

  const refresh = () => {
    renderer.toneMappingExposure = rig.settings.exposure;
    rig.update();
    applySurfaceSettings(getGlobeMaterial(), getShellMaterial(), materialSettings);
    updateReadout();
  };

  const applyPreset = (presetName) => {
    const resolvedName = LIGHTING_PRESETS[presetName] ? presetName : "studio";
    const preset = LIGHTING_PRESETS[resolvedName];

    for (const key of Object.keys(rig.settings)) {
      if (Object.prototype.hasOwnProperty.call(preset, key)) {
        rig.settings[key] = preset[key];
      }
    }

    const materialKeys = [
      "roughness",
      "clearcoat",
      "clearcoatRoughness",
      "envMapIntensity",
      "ior",
      "reflectivity",
      "specularIntensity",
      "sheen",
      "sheenRoughness",
      "sheenColor",
      "normalStrength",
      "shellIntensity"
    ];
    for (let i = 0; i < materialKeys.length; i += 1) {
      const key = materialKeys[i];
      if (Object.prototype.hasOwnProperty.call(preset, key)) {
        materialSettings[key] = preset[key];
      }
    }

    if (lightingPresetEl) {
      lightingPresetEl.value = resolvedName;
    }
    activePreset = resolvedName;
    updatePresetButtonsUI(activePreset);
    updatePresetLabel(formatPresetName(resolvedName));
    syncAllControls();
    refresh();
    return resolvedName;
  };

  for (let i = 0; i < controls.length; i += 1) {
    const def = controls[i];
    const input = document.querySelector(`#${def.id}`);
    if (!input) {
      continue;
    }
    const output = def.output ? document.querySelector(`#${def.output}`) : null;
    const binding = { def, input, output };
    bindings.push(binding);
    input.addEventListener("input", () => {
      def.set(Number(input.value));
      if (output) {
        output.textContent = def.fmt(def.get());
      }
      updatePresetLabel("Custom");
      refresh();
    });
  }

  if (lightingPresetEl) {
    lightingPresetEl.addEventListener("change", () => {
      applyPreset(lightingPresetEl.value);
    });
  }
  if (lightingResetEl) {
    lightingResetEl.addEventListener("click", () => {
      applyPreset("studio");
    });
  }

  applyPreset(lightingPresetEl?.value || REQUESTED_LIGHTING_PRESET || DEFAULT_LIGHTING_PRESET);
  if (typeof window !== "undefined") {
    window.setGlobeLightingPreset = (presetName) => {
      applyPreset(String(presetName || "").toLowerCase());
    };
    window.listGlobeLightingPresets = () => Object.keys(LIGHTING_PRESETS);
  }
  return {
    refresh,
    setPreset: applyPreset,
    getPreset: () => activePreset
  };
}

function setupPresetButtons(controlsApi) {
  if (!controlsApi || typeof controlsApi.setPreset !== "function") {
    return;
  }
  const buttons = Array.from(document.querySelectorAll(".lighting-preset-button"));
  if (!buttons.length) {
    return;
  }

  for (let i = 0; i < buttons.length; i += 1) {
    const button = buttons[i];
    button.addEventListener("click", () => {
      const presetName = String(button.dataset.preset || "").toLowerCase();
      controlsApi.setPreset(presetName);
    });
  }
  updatePresetButtonsUI(controlsApi.getPreset ? controlsApi.getPreset() : DEFAULT_LIGHTING_PRESET);
}

function updatePresetButtonsUI(activePreset) {
  const buttons = document.querySelectorAll(".lighting-preset-button");
  for (let i = 0; i < buttons.length; i += 1) {
    const button = buttons[i];
    const presetName = String(button.dataset.preset || "").toLowerCase();
    button.classList.toggle("is-active", presetName === activePreset);
  }
}

function createLightingRig(scene, camera, controls) {
  const settings = { ...LIGHTING_DEFAULTS };

  const ambient = new THREE.AmbientLight(0xffffff, settings.ambientIntensity);
  const hemi = new THREE.HemisphereLight(0xffffff, 0xffffff, settings.hemiIntensity);
  const front = new THREE.DirectionalLight(0xffffff, settings.frontIntensity);
  const key = new THREE.DirectionalLight(0xffffff, settings.keyIntensity);
  const fill = new THREE.DirectionalLight(0xffffff, settings.fillIntensity);
  const rim = new THREE.DirectionalLight(0xffffff, settings.rimIntensity);
  const accent = new THREE.DirectionalLight(0xffffff, settings.accentIntensity);
  scene.add(ambient, hemi, front, key, fill, rim, accent);
  scene.add(front.target, key.target, fill.target, rim.target, accent.target);

  const center = new THREE.Vector3();
  const fallbackCenter = new THREE.Vector3(0, 0, 0);
  const towardCamera = new THREE.Vector3();
  const right = new THREE.Vector3();
  const up = new THREE.Vector3();
  const dir = new THREE.Vector3();
  const fallbackRight = new THREE.Vector3(1, 0, 0);
  const worldUp = new THREE.Vector3(0, 1, 0);

  const place = (light, azimuthDeg, elevationDeg, distance) => {
    center.copy(controls ? controls.target : fallbackCenter);
    towardCamera.copy(camera.position).sub(center).normalize();
    if (towardCamera.lengthSq() < 1e-6) {
      towardCamera.set(0, 0, 1);
    }

    right.crossVectors(towardCamera, worldUp);
    if (right.lengthSq() < 1e-6) {
      right.copy(fallbackRight);
    } else {
      right.normalize();
    }

    up.crossVectors(right, towardCamera).normalize();

    dir.copy(towardCamera);
    dir.applyAxisAngle(up, THREE.MathUtils.degToRad(azimuthDeg));
    dir.applyAxisAngle(right, THREE.MathUtils.degToRad(elevationDeg));
    dir.normalize();

    light.position.copy(center).addScaledVector(dir, distance);
    light.target.position.copy(center);
    light.target.updateMatrixWorld();
    light.updateMatrixWorld();
  };

  const update = () => {
    ambient.intensity = settings.ambientIntensity;
    hemi.intensity = settings.hemiIntensity;
    front.intensity = settings.frontIntensity;
    key.intensity = settings.keyIntensity;
    fill.intensity = settings.fillIntensity;
    rim.intensity = settings.rimIntensity;
    accent.intensity = settings.accentIntensity;

    setColorFromKelvin(hemi.color, settings.hemiSkyTemp);
    setColorFromKelvin(hemi.groundColor, settings.hemiGroundTemp);
    setColorFromKelvin(front.color, settings.frontTemp);
    setColorFromKelvin(key.color, settings.keyTemp);
    setColorFromKelvin(fill.color, settings.fillTemp);
    setColorFromKelvin(rim.color, settings.rimTemp);
    setColorFromKelvin(accent.color, settings.accentTemp);

    place(front, 0, 0, settings.frontDistance);
    place(key, settings.keyAzimuth, settings.keyElevation, settings.keyDistance);
    place(fill, settings.fillAzimuth, settings.fillElevation, settings.fillDistance);
    place(rim, settings.rimAzimuth, settings.rimElevation, settings.rimDistance);
    place(accent, settings.accentAzimuth, settings.accentElevation, settings.accentDistance);
  };

  update();
  return { settings, update };
}

async function loadTexture(url) {
  const loader = new THREE.TextureLoader();
  loader.setCrossOrigin("anonymous");
  const texture = await loader.loadAsync(url);
  texture.needsUpdate = true;
  return texture;
}

async function loadTextureWithFallback(urls, { timeoutMs = 18000 } = {}) {
  if (!Array.isArray(urls) || !urls.length) {
    throw new Error("No texture URLs provided");
  }

  let latestError;
  for (let i = 0; i < urls.length; i += 1) {
    const url = urls[i];
    try {
      return await withTimeout(
        loadTexture(url),
        timeoutMs,
        `Texture load timed out: ${url}`
      );
    } catch (error) {
      latestError = error;
      console.warn(`Texture fallback failed: ${url}`, error);
    }
  }

  throw latestError || new Error("Unable to load texture from fallback list.");
}

async function fetchJsonOrNull(url, { timeoutMs = 12000 } = {}) {
  const controller = typeof AbortController !== "undefined" ? new AbortController() : null;
  const timeoutId = setTimeout(() => {
    if (controller) {
      controller.abort();
    }
  }, timeoutMs);

  try {
    const response = await fetch(url, controller ? { signal: controller.signal } : undefined);
    if (!response.ok) {
      throw new Error(`Request failed: ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.warn(`Unable to load ${url}`, error);
    return null;
  } finally {
    clearTimeout(timeoutId);
  }
}

async function fetchJsonWithFallback(urls, { timeoutMs = 12000 } = {}) {
  if (!Array.isArray(urls) || !urls.length) {
    return null;
  }

  for (let i = 0; i < urls.length; i += 1) {
    const data = await fetchJsonOrNull(urls[i], { timeoutMs });
    if (data) {
      return data;
    }
  }
  return null;
}

function createCanvasTexture(
  canvas,
  {
    colorSpace = THREE.NoColorSpace,
    anisotropy = 1,
    wrapS = THREE.ClampToEdgeWrapping,
    wrapT = THREE.ClampToEdgeWrapping
  } = {}
) {
  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = colorSpace;
  texture.wrapS = wrapS;
  texture.wrapT = wrapT;
  texture.anisotropy = anisotropy;
  texture.minFilter = THREE.LinearMipmapLinearFilter;
  texture.magFilter = THREE.LinearFilter;
  texture.needsUpdate = true;
  return texture;
}

function withTimeout(promise, timeoutMs, timeoutMessage) {
  let timeoutId;
  return new Promise((resolve, reject) => {
    timeoutId = setTimeout(() => {
      reject(new Error(timeoutMessage || "Operation timed out"));
    }, timeoutMs);

    promise
      .then((value) => {
        clearTimeout(timeoutId);
        resolve(value);
      })
      .catch((error) => {
        clearTimeout(timeoutId);
        reject(error);
      });
  });
}

function waitForNextFrame() {
  return new Promise((resolve) => {
    requestAnimationFrame(() => resolve());
  });
}

function createHeightSampler(heightImage, width, height) {
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d", { willReadFrequently: true });
  if (!ctx) {
    throw new Error("Unable to allocate elevation sampling canvas");
  }

  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = "high";
  ctx.drawImage(heightImage, 0, 0, width, height);

  let rgbaData;
  try {
    rgbaData = ctx.getImageData(0, 0, width, height).data;
  } catch (error) {
    throw new Error("Unable to read elevation texture data");
  }
  const data = extractLuminanceChannel(rgbaData, width, height);
  canvas.width = 1;
  canvas.height = 1;

  return {
    width,
    height,
    data,
    sample(u, v) {
      return sampleScalarBilinear(data, width, height, u, v);
    }
  };
}

function extractRedChannel(rgbaData, width, height) {
  const size = width * height;
  const out = new Uint8Array(size);
  for (let i = 0, p = 0; p < size; i += 4, p += 1) {
    out[p] = rgbaData[i];
  }
  return out;
}

function extractLuminanceChannel(rgbaData, width, height) {
  const size = width * height;
  const out = new Uint8Array(size);
  for (let i = 0, p = 0; p < size; i += 4, p += 1) {
    out[p] = Math.round(rgbaData[i] * 0.2126 + rgbaData[i + 1] * 0.7152 + rgbaData[i + 2] * 0.0722);
  }
  return out;
}

function sampleScalarBilinear(data, width, height, u, v) {
  const wrappedU = ((u % 1) + 1) % 1;
  const clampedV = clamp(v, 0, 1);
  const x = wrappedU * (width - 1);
  const y = clampedV * (height - 1);

  const x0 = Math.floor(x);
  const y0 = Math.floor(y);
  const x1 = Math.min(width - 1, x0 + 1);
  const y1 = Math.min(height - 1, y0 + 1);
  const tx = x - x0;
  const ty = y - y0;

  const l00 = data[y0 * width + x0];
  const l10 = data[y0 * width + x1];
  const l01 = data[y1 * width + x0];
  const l11 = data[y1 * width + x1];

  const top = mix(l00, l10, tx);
  const bottom = mix(l01, l11, tx);
  return mix(top, bottom, ty) / 255;
}

function sampleMaskNearest(maskData, width, height, u, v) {
  if (!maskData) {
    return 0;
  }
  const wrappedU = ((u % 1) + 1) % 1;
  const clampedV = clamp(v, 0, 1);
  const x = clamp(Math.round(wrappedU * (width - 1)), 0, width - 1);
  const y = clamp(Math.round(clampedV * (height - 1)), 0, height - 1);
  return maskData[y * width + x] / 255;
}

function decodeSignedElevation(heightValue) {
  const seaLevel = 0.5;
  if (heightValue >= seaLevel) {
    return (heightValue - seaLevel) / Math.max(1e-6, 1 - seaLevel);
  }
  return (heightValue - seaLevel) / Math.max(1e-6, seaLevel);
}

function computeLandRelief(heightValue, landWeight = 1) {
  const signedElevation = decodeSignedElevation(heightValue);
  const landElevation = Math.max(0, signedElevation);

  // Base relief signal stored per-vertex. Final sharpness is applied per-frame
  // in displacement shaping so it can respond to terrain exaggeration.
  const base = Math.pow(landElevation, 0.98);
  const summitMask = smoothstep(0.56, 0.98, landElevation);
  const summitBoost = Math.pow(summitMask, 2.4) * 0.33;
  const sharpened = clamp01(base + summitBoost);
  return sharpened * landWeight;
}

function computeShapedReliefDisplacement(reliefValue, exaggeration, contrast) {
  const r = clamp01(reliefValue);
  const ex = clamp(Number(exaggeration) || 1, TERRAIN_EXAGGERATION_MIN, TERRAIN_EXAGGERATION_MAX);
  const ctr = clamp(Number(contrast) || 1, 0.35, 2.5);
  const reliefGain = 0.72 + ctr * 0.28;

  // When exaggeration is low, push sharpness into high peaks only.
  const lowExBoost = clamp((1 - ex) / Math.max(1e-6, 1 - TERRAIN_EXAGGERATION_MIN), 0, 1);
  const peakMask = smoothstep(0.58, 0.985, r);
  const ridgeMask = smoothstep(0.42, 0.9, r);
  const summitLift = Math.pow(peakMask, 2.5) * (0.02 + lowExBoost * 0.16);
  const ridgeCut = Math.pow(ridgeMask, 1.8) * peakMask * (0.03 + lowExBoost * 0.07);
  const shapedRelief = clamp01(r + summitLift - ridgeCut);
  const apexMask = smoothstep(0.86, 1, shapedRelief);
  const apexClip = Math.pow(apexMask, 1.35) * (0.026 + lowExBoost * 0.034);
  const softenedRelief = clamp01(shapedRelief - apexClip);
  const peakCompensation = 1 + lowExBoost * Math.pow(peakMask, 1.6) * 0.16;

  return softenedRelief * BASE_RELIEF.displacementScale * ex * reliefGain * peakCompensation;
}

function buildReliefMesh({
  radius,
  widthSegments,
  heightSegments,
  heightSampler,
  landMaskData,
  landMaskWidth,
  landMaskHeight,
  lakeMaskData,
  lakeMaskWidth,
  lakeMaskHeight,
  terrainExaggeration,
  normalStrength
}) {
  const geometry = new THREE.SphereGeometry(radius, widthSegments, heightSegments);
  const positionAttr = geometry.attributes.position;
  const vertexCount = positionAttr.count;
  const positions = positionAttr.array;
  const directions = new Float32Array(positions.length);
  const relief = new Float32Array(vertexCount);
  const colors = new Float32Array(vertexCount * 3);

  for (let i = 0; i < vertexCount; i += 1) {
    const i3 = i * 3;
    const x = positions[i3];
    const y = positions[i3 + 1];
    const z = positions[i3 + 2];
    const len = Math.hypot(x, y, z) || 1;
    const dx = x / len;
    const dy = y / len;
    const dz = z / len;
    directions[i3] = dx;
    directions[i3 + 1] = dy;
    directions[i3 + 2] = dz;

    // Use geographic lon/lat derived from the vertex direction so terrain and
    // vector borders share the exact same reference frame.
    const lon = Math.atan2(dz, dx) * LONGITUDE_ORIENTATION;
    const lat = Math.asin(clamp(dy, -1, 1));
    const u = lon / (Math.PI * 2) + 0.5;
    const v = 0.5 - lat / Math.PI;
    const heightValue = heightSampler.sample(u, v);
    const inferredLand = heightValue > 0.5002 ? 1 : 0;
    const sampledLand = sampleMaskNearest(landMaskData, landMaskWidth, landMaskHeight, u, v);
    const land = landMaskData ? sampledLand : inferredLand;
    const sampledLake = sampleMaskNearest(lakeMaskData, lakeMaskWidth, lakeMaskHeight, u, v);
    const lake = lakeMaskData ? sampledLake : 0;
    const lakeWeight = clamp01(lake * land);
    const effectiveLand = land * (1 - lakeWeight * 0.96);
    const landRelief = computeLandRelief(heightValue, effectiveLand);
    relief[i] = landRelief;

    const signedElevation = decodeSignedElevation(heightValue);
    const centeredHeight = Math.tanh(signedElevation * 3.2);
    const latNorm = Math.abs(Math.sin(lat));
    const equatorWarmth = 1 - latNorm;
    const zonalBand = Math.sin(lon + lat * 0.35 + 1.15);
    const continentalWarmth = land * 0.16;
    const altitudeCooling = Math.max(0, centeredHeight) * land * 0.42;

    let temperature =
      0.5 +
      zonalBand * 0.26 +
      equatorWarmth * 0.2 +
      continentalWarmth -
      altitudeCooling;
    temperature = clamp01(temperature);

    const tempColor = sampleTemperatureRamp(temperature);
    const porcelain = { r: 0.93, g: 0.94, b: 0.955 };
    const lakeColor = { r: 0.53, g: 0.69, b: 0.88 };
    const tempMix = land * (0.34 + land * 0.46);
    const mountainTint = landRelief * land * 0.22;
    const oceanColor = { r: 0.992, g: 0.992, b: 0.992 };

    let outR = mix(porcelain.r, tempColor.r, tempMix);
    let outG = mix(porcelain.g, tempColor.g, tempMix);
    let outB = mix(porcelain.b, tempColor.b, tempMix);
    outR += mountainTint * 0.08;
    outG += mountainTint * 0.1;
    outB += mountainTint * 0.18;
    outR = mix(outR, lakeColor.r, lakeWeight * 0.92);
    outG = mix(outG, lakeColor.g, lakeWeight * 0.92);
    outB = mix(outB, lakeColor.b, lakeWeight * 0.92);
    outR = mix(oceanColor.r, outR, land);
    outG = mix(oceanColor.g, outG, land);
    outB = mix(oceanColor.b, outB, land);

    colors[i3] = clamp01(outR);
    colors[i3 + 1] = clamp01(outG);
    colors[i3 + 2] = clamp01(outB);
  }

  geometry.setAttribute("color", new THREE.BufferAttribute(colors, 3));

  const material = new THREE.MeshPhysicalMaterial({
    color: 0xffffff,
    vertexColors: true,
    roughness: MATERIAL_DEFAULTS.roughness,
    metalness: 0,
    clearcoat: MATERIAL_DEFAULTS.clearcoat,
    clearcoatRoughness: MATERIAL_DEFAULTS.clearcoatRoughness,
    envMapIntensity: MATERIAL_DEFAULTS.envMapIntensity
  });
  material.dithering = true;
  if ("ior" in material) {
    material.ior = MATERIAL_DEFAULTS.ior;
  }
  if ("reflectivity" in material) {
    material.reflectivity = MATERIAL_DEFAULTS.reflectivity;
  }
  if ("specularIntensity" in material) {
    material.specularIntensity = MATERIAL_DEFAULTS.specularIntensity;
  }
  if ("sheen" in material) {
    material.sheen = MATERIAL_DEFAULTS.sheen;
  }
  if ("sheenRoughness" in material) {
    material.sheenRoughness = MATERIAL_DEFAULTS.sheenRoughness;
  }
  if ("sheenColor" in material) {
    material.sheenColor = new THREE.Color(MATERIAL_DEFAULTS.sheenColor);
  }
  material.userData.reliefState = {
    terrainExaggeration: null,
    normalStrength: null
  };

  const mesh = new THREE.Mesh(geometry, material);
  mesh.castShadow = false;
  mesh.receiveShadow = false;

  const updateTerrain = (factor, reliefContrast = 1) => {
    const exaggeration = clamp(Number(factor) || 1, TERRAIN_EXAGGERATION_MIN, TERRAIN_EXAGGERATION_MAX);
    const contrast = clamp(Number(reliefContrast) || 1, 0.35, 2.5);

    for (let i = 0; i < vertexCount; i += 1) {
      const i3 = i * 3;
      const displacement = computeShapedReliefDisplacement(relief[i], exaggeration, contrast);
      const displacedRadius = radius + displacement;
      positions[i3] = directions[i3] * displacedRadius;
      positions[i3 + 1] = directions[i3 + 1] * displacedRadius;
      positions[i3 + 2] = directions[i3 + 2] * displacedRadius;
    }

    positionAttr.needsUpdate = true;
    geometry.computeVertexNormals();
    geometry.computeBoundingSphere();
  };

  updateTerrain(terrainExaggeration, normalStrength);
  return { mesh, material, updateTerrain };
}

function buildVectorBorderGroup({
  countriesGeo,
  stateLinesGeo,
  riversGeo,
  lakesGeo,
  radius,
  heightSampler,
  landMaskData,
  landMaskWidth,
  landMaskHeight,
  terrainExaggeration,
  normalStrength,
  countryStepDeg = 1.15,
  stateStepDeg = 2.2,
  riverStepDeg = 0.8,
  lakeStepDeg = 0.9
}) {
  const group = new THREE.Group();
  const layers = [];

  const countryLayer = createVectorBorderLayer({
    geojson: countriesGeo,
    mode: "polygon",
    radius,
    heightSampler,
    landMaskData,
    landMaskWidth,
    landMaskHeight,
    color: 0xffffff,
    opacity: 0.58,
    offset: 0.0028,
    lift: 1.1,
    maxStepDeg: countryStepDeg
  });
  if (countryLayer) {
    group.add(countryLayer.line);
    layers.push(countryLayer);
  }

  const stateHaloRight = createVectorBorderLayer({
    geojson: stateLinesGeo,
    mode: "line",
    radius,
    heightSampler,
    landMaskData,
    landMaskWidth,
    landMaskHeight,
    color: 0xe8dfd0,
    opacity: 0.26,
    offset: 0.0032,
    lift: 1.11,
    maxStepDeg: stateStepDeg,
    surfaceOffsetRight: 0.00145
  });
  if (stateHaloRight) {
    group.add(stateHaloRight.line);
    layers.push(stateHaloRight);
  }

  const stateHaloLeft = createVectorBorderLayer({
    geojson: stateLinesGeo,
    mode: "line",
    radius,
    heightSampler,
    landMaskData,
    landMaskWidth,
    landMaskHeight,
    color: 0xe8dfd0,
    opacity: 0.26,
    offset: 0.0032,
    lift: 1.11,
    maxStepDeg: stateStepDeg,
    surfaceOffsetRight: -0.00145
  });
  if (stateHaloLeft) {
    group.add(stateHaloLeft.line);
    layers.push(stateHaloLeft);
  }

  const stateLayer = createVectorBorderLayer({
    geojson: stateLinesGeo,
    mode: "line",
    radius,
    heightSampler,
    landMaskData,
    landMaskWidth,
    landMaskHeight,
    color: 0xe8dfd0,
    opacity: 0.64,
    offset: 0.0033,
    lift: 1.12,
    maxStepDeg: stateStepDeg
  });
  if (stateLayer) {
    group.add(stateLayer.line);
    layers.push(stateLayer);
  }

  const riverFeatureFilter = (feature) => {
    const props = feature?.properties || {};
    const featureClass = String(props.featurecla || "").toLowerCase();
    if (featureClass && featureClass !== "river") {
      return false;
    }
    return passesScaleRank(props.scalerank, 9);
  };

  const riverCoreLayer = createVectorBorderLayer({
    geojson: riversGeo,
    mode: "line",
    radius,
    heightSampler,
    landMaskData,
    landMaskWidth,
    landMaskHeight,
    color: 0xffffff,
    opacity: 0.38,
    offset: 0.00205,
    lift: 1.06,
    maxStepDeg: riverStepDeg,
    featureFilter: riverFeatureFilter
  });
  if (riverCoreLayer) {
    group.add(riverCoreLayer.line);
    layers.push(riverCoreLayer);
  }

  const lakeLayer = createVectorBorderLayer({
    geojson: lakesGeo,
    mode: "polygon",
    radius,
    heightSampler,
    landMaskData,
    landMaskWidth,
    landMaskHeight,
    color: 0x68a7ea,
    opacity: 0.72,
    offset: 0.0019,
    lift: 1.04,
    maxStepDeg: lakeStepDeg,
    featureFilter: (feature) => {
      const props = feature?.properties || {};
      return passesScaleRank(props.scalerank, 8);
    }
  });
  if (lakeLayer) {
    group.add(lakeLayer.line);
    layers.push(lakeLayer);
  }

  const updateTerrain = (factor, reliefContrast = 1) => {
    const exaggeration = clamp(Number(factor) || 1, TERRAIN_EXAGGERATION_MIN, TERRAIN_EXAGGERATION_MAX);
    const contrast = clamp(Number(reliefContrast) || 1, 0.35, 2.5);

    for (let l = 0; l < layers.length; l += 1) {
      const layer = layers[l];
      const positions = layer.positions;
      for (let i = 0; i < layer.vertexCount; i += 1) {
        const i3 = i * 3;
        const displacement =
          computeShapedReliefDisplacement(layer.relief[i], exaggeration, contrast) * layer.lift;
        const displacedRadius = radius + displacement + layer.offset;
        positions[i3] = layer.directions[i3] * displacedRadius;
        positions[i3 + 1] = layer.directions[i3 + 1] * displacedRadius;
        positions[i3 + 2] = layer.directions[i3 + 2] * displacedRadius;
      }
      layer.positionAttr.needsUpdate = true;
      layer.geometry.computeBoundingSphere();
    }
  };

  updateTerrain(terrainExaggeration, normalStrength);
  return {
    group: layers.length ? group : null,
    updateTerrain: layers.length ? updateTerrain : null
  };
}

function createVectorBorderLayer({
  geojson,
  mode,
  radius,
  heightSampler,
  landMaskData,
  landMaskWidth,
  landMaskHeight,
  color,
  opacity,
  offset,
  lift = 1,
  maxStepDeg,
  featureFilter = null,
  materialOptions = null,
  surfaceOffsetRight = 0,
  surfaceOffsetUp = 0
}) {
  if (!geojson) {
    return null;
  }

  const segments = [];
  if (mode === "polygon") {
    forEachPolygon(geojson, (polygonRings) => {
      for (let r = 0; r < polygonRings.length; r += 1) {
        appendDensifiedLineSegments(polygonRings[r], segments, maxStepDeg);
      }
    }, featureFilter);
  } else {
    forEachLineString(geojson, (lineCoords) => {
      appendDensifiedLineSegments(lineCoords, segments, maxStepDeg);
    }, featureFilter);
  }

  if (!segments.length) {
    return null;
  }

  const vertexCount = segments.length / 2;
  const positions = new Float32Array(vertexCount * 3);
  const directions = new Float32Array(vertexCount * 3);
  const relief = new Float32Array(vertexCount);

  for (let i = 0; i < vertexCount; i += 1) {
    const lon = segments[i * 2];
    const lat = segments[i * 2 + 1];
    const direction = lonLatToDirection(lon, lat);
    const shiftedDirection = offsetDirectionInTangentFrame(
      direction,
      surfaceOffsetRight,
      surfaceOffsetUp
    );
    const sampleLon =
      Math.atan2(shiftedDirection.z, shiftedDirection.x) * THREE.MathUtils.RAD2DEG * LONGITUDE_ORIENTATION;
    const sampleLat = Math.asin(clamp(shiftedDirection.y, -1, 1)) * THREE.MathUtils.RAD2DEG;
    const u = (normalizeLon(sampleLon) + 180) / 360;
    const v = (90 - sampleLat) / 180;
    const heightValue = heightSampler.sample(u, v);
    const inferredLand = heightValue > 0.5002 ? 1 : 0;
    const sampledLand = sampleMaskNearest(landMaskData, landMaskWidth, landMaskHeight, u, v);
    const land = landMaskData ? sampledLand : inferredLand;

    directions[i * 3] = shiftedDirection.x;
    directions[i * 3 + 1] = shiftedDirection.y;
    directions[i * 3 + 2] = shiftedDirection.z;
    relief[i] = computeLandRelief(heightValue, land);
  }

  const geometry = new THREE.BufferGeometry();
  const positionAttr = new THREE.BufferAttribute(positions, 3);
  geometry.setAttribute("position", positionAttr);
  const material = new THREE.LineBasicMaterial({
    color,
    transparent: true,
    opacity,
    depthTest: true,
    depthWrite: false,
    ...(materialOptions || {})
  });
  const line = new THREE.LineSegments(geometry, material);

  return {
    line,
    geometry,
    positionAttr,
    positions,
    directions,
    relief,
    vertexCount,
    offset,
    lift
  };
}

function appendDensifiedLineSegments(lineCoords, outLonLatPairs, maxStepDeg = 0.75) {
  const unwrapped = unwrapCoordinates(lineCoords);
  if (unwrapped.length < 2) {
    return;
  }

  for (let i = 1; i < unwrapped.length; i += 1) {
    const a = unwrapped[i - 1];
    const b = unwrapped[i];
    const lonA = a[0];
    const latA = a[1];
    const lonB = b[0];
    const latB = b[1];
    const span = Math.max(Math.abs(lonB - lonA), Math.abs(latB - latA));
    const steps = Math.max(1, Math.min(96, Math.ceil(span / maxStepDeg)));

    let prevLon = lonA;
    let prevLat = latA;
    for (let s = 1; s <= steps; s += 1) {
      const t = s / steps;
      const lon = mix(lonA, lonB, t);
      const lat = mix(latA, latB, t);

      outLonLatPairs.push(prevLon, prevLat);
      outLonLatPairs.push(lon, lat);
      prevLon = lon;
      prevLat = lat;
    }
  }
}

function lonLatToDirection(lonDeg, latDeg) {
  const lon = THREE.MathUtils.degToRad(lonDeg * LONGITUDE_ORIENTATION);
  const lat = THREE.MathUtils.degToRad(latDeg);
  const cosLat = Math.cos(lat);
  return new THREE.Vector3(
    cosLat * Math.cos(lon),
    Math.sin(lat),
    cosLat * Math.sin(lon)
  );
}

function offsetDirectionInTangentFrame(direction, offsetRight = 0, offsetUp = 0) {
  if (Math.abs(offsetRight) < 1e-8 && Math.abs(offsetUp) < 1e-8) {
    return direction;
  }

  let rx = direction.z;
  let ry = 0;
  let rz = -direction.x;
  let rLen = Math.hypot(rx, ry, rz);
  if (rLen < 1e-6) {
    rx = 0;
    ry = -direction.z;
    rz = direction.y;
    rLen = Math.hypot(rx, ry, rz);
  }
  rx /= rLen || 1;
  ry /= rLen || 1;
  rz /= rLen || 1;

  const ux = direction.y * rz - direction.z * ry;
  const uy = direction.z * rx - direction.x * rz;
  const uz = direction.x * ry - direction.y * rx;

  let sx = direction.x + rx * offsetRight + ux * offsetUp;
  let sy = direction.y + ry * offsetRight + uy * offsetUp;
  let sz = direction.z + rz * offsetRight + uz * offsetUp;
  const sLen = Math.hypot(sx, sy, sz) || 1;
  sx /= sLen;
  sy /= sLen;
  sz /= sLen;

  return new THREE.Vector3(sx, sy, sz);
}

function buildLandMaskCanvas(countriesGeo, width, height) {
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    return canvas;
  }

  ctx.fillStyle = "black";
  ctx.fillRect(0, 0, width, height);

  if (!countriesGeo) {
    // Fail safe: unknown mask defaults to ocean, not land.
    console.warn("Country GeoJSON unavailable; land mask fallback set to ocean.");
    return canvas;
  }

  ctx.fillStyle = "white";
  forEachPolygon(countriesGeo, (polygonRings) => {
    fillWrappedPolygon(ctx, polygonRings, width, height);
  });

  return canvas;
}

function buildLakeMaskCanvas(lakesGeo, width, height) {
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    return canvas;
  }

  ctx.fillStyle = "black";
  ctx.fillRect(0, 0, width, height);
  if (!lakesGeo) {
    return canvas;
  }

  ctx.fillStyle = "white";
  forEachPolygon(lakesGeo, (polygonRings) => {
    fillWrappedPolygon(ctx, polygonRings, width, height);
  });
  return canvas;
}

function buildBorderCanvas(countriesGeo, stateLinesGeo, width, height) {
  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    return canvas;
  }

  ctx.clearRect(0, 0, width, height);
  ctx.lineJoin = "round";
  ctx.lineCap = "round";

  const countryWidth = Math.max(0.7, width * 0.00046);
  const stateWidth = Math.max(0.35, width * 0.0003);

  if (countriesGeo) {
    ctx.strokeStyle = "rgba(38, 46, 60, 0.58)";
    ctx.lineWidth = countryWidth;
    forEachPolygon(countriesGeo, (polygonRings) => {
      for (let ringIndex = 0; ringIndex < polygonRings.length; ringIndex += 1) {
        strokeWrappedLine(ctx, polygonRings[ringIndex], width, height);
      }
    });
  }

  if (stateLinesGeo) {
    ctx.strokeStyle = "rgba(44, 52, 66, 0.3)";
    ctx.lineWidth = stateWidth;
    forEachLineString(stateLinesGeo, (lineCoords) => {
      strokeWrappedLine(ctx, lineCoords, width, height);
    });
  }

  return canvas;
}

function buildSurfaceCanvases({ heightImage, landMaskCanvas, borderCanvas, width, height }) {
  const heightCanvas = document.createElement("canvas");
  heightCanvas.width = width;
  heightCanvas.height = height;
  const heightCtx = heightCanvas.getContext("2d", { willReadFrequently: true });
  const landCtx = landMaskCanvas.getContext("2d", { willReadFrequently: true });
  const borderCtx = borderCanvas.getContext("2d", { willReadFrequently: true });

  const albedoCanvas = document.createElement("canvas");
  albedoCanvas.width = width;
  albedoCanvas.height = height;
  const albedoCtx = albedoCanvas.getContext("2d");

  const displacementCanvas = document.createElement("canvas");
  displacementCanvas.width = width;
  displacementCanvas.height = height;
  const displacementCtx = displacementCanvas.getContext("2d");

  const bumpCanvas = document.createElement("canvas");
  bumpCanvas.width = width;
  bumpCanvas.height = height;
  const bumpCtx = bumpCanvas.getContext("2d");

  if (!heightCtx || !landCtx || !borderCtx || !albedoCtx || !displacementCtx || !bumpCtx) {
    return { albedoCanvas, displacementCanvas, bumpCanvas };
  }

  heightCtx.drawImage(heightImage, 0, 0, width, height);

  let heightData;
  let landData;
  let borderData;
  try {
    heightData = heightCtx.getImageData(0, 0, width, height).data;
    landData = landCtx.getImageData(0, 0, width, height).data;
    borderData = borderCtx.getImageData(0, 0, width, height).data;
  } catch (error) {
    console.warn("Texture synthesis skipped due canvas restrictions.", error);
    return { albedoCanvas, displacementCanvas, bumpCanvas };
  }

  const albedoImage = new ImageData(width, height);
  const displacementImage = new ImageData(width, height);
  const bumpImage = new ImageData(width, height);

  const outAlbedo = albedoImage.data;
  const outDisplacement = displacementImage.data;
  const outBump = bumpImage.data;
  const seaLevelValue = 0.5;

  for (let y = 0; y < height; y += 1) {
    const v = y / Math.max(1, height - 1);
    const lat = (0.5 - v) * Math.PI;
    const latNorm = Math.abs(Math.sin(lat));

    for (let x = 0; x < width; x += 1) {
      const i = (y * width + x) * 4;

      const r = heightData[i] / 255;
      const g = heightData[i + 1] / 255;
      const b = heightData[i + 2] / 255;
      const heightValue = r * 0.2126 + g * 0.7152 + b * 0.0722;

      const land = landData[i] / 255;
      const border = borderData[i + 3] / 255;

      const u = x / Math.max(1, width - 1);
      const lon = (u * 2 - 1) * Math.PI;

      const signedElevation =
        heightValue >= seaLevelValue
          ? (heightValue - seaLevelValue) / Math.max(1e-6, 1 - seaLevelValue)
          : (heightValue - seaLevelValue) / Math.max(1e-6, seaLevelValue);
      const centeredHeight = Math.tanh(signedElevation * 3.2);
      const aboveSea = Math.max(0, signedElevation);
      const landRelief = Math.pow(aboveSea, 0.62);
      const polarFade = 1 - smoothstep(0.92, 0.995, latNorm);

      // Ocean is intentionally flat at sea level.
      let displacement =
        0.5 +
        landRelief * 0.5;
      displacement -= border * land * 0.006;
      displacement = 0.5 + (displacement - 0.5) * polarFade;
      displacement = clamp01(displacement);

      let bump =
        0.5 +
        landRelief * (0.22 + land * 0.54) -
        border * land * 0.12;
      bump = 0.5 + (bump - 0.5) * polarFade;
      bump = clamp01(bump);

      const equatorWarmth = 1 - latNorm;
      const zonalBand = Math.sin(lon + lat * 0.35 + 1.15);
      const continentalWarmth = land * 0.16;
      const altitudeCooling = Math.max(0, centeredHeight) * land * 0.42;

      let temperature =
        0.5 +
        zonalBand * 0.26 +
        equatorWarmth * 0.2 +
        continentalWarmth -
        altitudeCooling;
      temperature = clamp01(temperature);

      const tempColor = sampleTemperatureRamp(temperature);
      const porcelain = {
        r: 0.93,
        g: 0.94,
        b: 0.955
      };

      const tempMix = land * (0.34 + land * 0.46);
      const carvedShade = border * land * (0.14 + land * 0.08);
      const valleyShade = Math.max(0, -centeredHeight) * (0.028 + land * 0.018);
      const mountainTint = landRelief * land * 0.22;
      const oceanWhiteness = 0.992;

      let outR = mix(porcelain.r, tempColor.r, tempMix) - carvedShade * 0.7 - valleyShade;
      let outG = mix(porcelain.g, tempColor.g, tempMix) - carvedShade * 0.58 - valleyShade;
      let outB = mix(porcelain.b, tempColor.b, tempMix) - carvedShade * 0.45 - valleyShade;
      outR += mountainTint * 0.08;
      outG += mountainTint * 0.1;
      outB += mountainTint * 0.18;
      outR = mix(oceanWhiteness, outR, land);
      outG = mix(oceanWhiteness, outG, land);
      outB = mix(oceanWhiteness, outB, land);

      outR = clamp01(outR);
      outG = clamp01(outG);
      outB = clamp01(outB);

      const d = Math.round(displacement * 255);
      outDisplacement[i] = d;
      outDisplacement[i + 1] = d;
      outDisplacement[i + 2] = d;
      outDisplacement[i + 3] = 255;

      const bp = Math.round(bump * 255);
      outBump[i] = bp;
      outBump[i + 1] = bp;
      outBump[i + 2] = bp;
      outBump[i + 3] = 255;

      outAlbedo[i] = Math.round(outR * 255);
      outAlbedo[i + 1] = Math.round(outG * 255);
      outAlbedo[i + 2] = Math.round(outB * 255);
      outAlbedo[i + 3] = 255;
    }
  }

  blendTextureSeam(outAlbedo, width, height, 4);
  blendTextureSeam(outDisplacement, width, height, 4);
  blendTextureSeam(outBump, width, height, 4);

  albedoCtx.putImageData(albedoImage, 0, 0);
  displacementCtx.putImageData(displacementImage, 0, 0);
  bumpCtx.putImageData(bumpImage, 0, 0);

  return { albedoCanvas, displacementCanvas, bumpCanvas };
}

function blendTextureSeam(buffer, width, height, channels) {
  const seamBlendColumns = Math.min(4, Math.max(1, Math.floor(width / 1024)));
  for (let y = 0; y < height; y += 1) {
    for (let c = 0; c < seamBlendColumns; c += 1) {
      const leftIndex = (y * width + c) * channels;
      const rightIndex = (y * width + (width - 1 - c)) * channels;
      for (let ch = 0; ch < channels; ch += 1) {
        const avg = Math.round((buffer[leftIndex + ch] + buffer[rightIndex + ch]) * 0.5);
        buffer[leftIndex + ch] = avg;
        buffer[rightIndex + ch] = avg;
      }
    }
  }
}

function sampleTemperatureRamp(value) {
  const cold = { r: 0.2, g: 0.49, b: 1.0 };
  const neutral = { r: 0.95, g: 0.96, b: 0.97 };
  const warm = { r: 0.97, g: 0.72, b: 0.39 };

  if (value <= 0.5) {
    // Hold onto the cold endpoint longer so blues stay richer.
    const t = Math.pow(value / 0.5, 1.36);
    return {
      r: mix(cold.r, neutral.r, t),
      g: mix(cold.g, neutral.g, t),
      b: mix(cold.b, neutral.b, t)
    };
  }

  const t = (value - 0.5) / 0.5;
  return {
    r: mix(neutral.r, warm.r, t),
    g: mix(neutral.g, warm.g, t),
    b: mix(neutral.b, warm.b, t)
  };
}

function fillWrappedPolygon(ctx, polygonRings, width, height) {
  if (!Array.isArray(polygonRings) || !polygonRings.length) {
    return;
  }

  const offsets = [-width, 0, width];
  for (let o = 0; o < offsets.length; o += 1) {
    const offsetX = offsets[o];
    ctx.beginPath();
    for (let ringIndex = 0; ringIndex < polygonRings.length; ringIndex += 1) {
      traceWrappedLine(ctx, polygonRings[ringIndex], width, height, offsetX, true);
    }
    ctx.fill("evenodd");
  }
}

function strokeWrappedLine(ctx, lineCoords, width, height) {
  const offsets = [-width, 0, width];
  for (let o = 0; o < offsets.length; o += 1) {
    ctx.beginPath();
    traceWrappedLine(ctx, lineCoords, width, height, offsets[o], false);
    ctx.stroke();
  }
}

function traceWrappedLine(ctx, lineCoords, width, height, offsetX = 0, closePath = false) {
  const unwrapped = unwrapCoordinates(lineCoords);
  if (unwrapped.length < 2) {
    return;
  }

  for (let i = 0; i < unwrapped.length; i += 1) {
    const point = unwrapped[i];
    const lon = point[0];
    const lat = point[1];

    const x = ((lon + 180) / 360) * width + offsetX;
    const y = ((90 - lat) / 180) * height;

    if (i === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  }

  if (closePath) {
    ctx.closePath();
  }
}

function unwrapCoordinates(coords) {
  if (!Array.isArray(coords)) {
    return [];
  }

  const result = [];
  let prevLon = null;

  for (let i = 0; i < coords.length; i += 1) {
    const point = coords[i];
    if (!Array.isArray(point) || point.length < 2) {
      continue;
    }

    let lon = Number(point[0]);
    const lat = Number(point[1]);
    if (!Number.isFinite(lon) || !Number.isFinite(lat)) {
      continue;
    }

    lon = normalizeLon(lon);

    if (prevLon !== null) {
      while (lon - prevLon > 180) {
        lon -= 360;
      }
      while (lon - prevLon < -180) {
        lon += 360;
      }
    }

    result.push([lon, clamp(lat, -90, 90)]);
    prevLon = lon;
  }

  return result;
}

function normalizeLon(lon) {
  let normalized = lon;
  while (normalized > 180) {
    normalized -= 360;
  }
  while (normalized < -180) {
    normalized += 360;
  }
  return normalized;
}

function forEachPolygon(geojson, visit, featureFilter = null) {
  const features = Array.isArray(geojson?.features) ? geojson.features : [];
  for (let i = 0; i < features.length; i += 1) {
    const feature = features[i];
    if (typeof featureFilter === "function" && !featureFilter(feature)) {
      continue;
    }
    const geometry = feature?.geometry;
    if (!geometry) {
      continue;
    }

    if (geometry.type === "Polygon") {
      if (Array.isArray(geometry.coordinates)) {
        visit(geometry.coordinates, feature);
      }
    } else if (geometry.type === "MultiPolygon") {
      const polygons = Array.isArray(geometry.coordinates) ? geometry.coordinates : [];
      for (let p = 0; p < polygons.length; p += 1) {
        if (Array.isArray(polygons[p])) {
          visit(polygons[p], feature);
        }
      }
    }
  }
}

function forEachLineString(geojson, visit, featureFilter = null) {
  const features = Array.isArray(geojson?.features) ? geojson.features : [];
  for (let i = 0; i < features.length; i += 1) {
    const feature = features[i];
    if (typeof featureFilter === "function" && !featureFilter(feature)) {
      continue;
    }
    const geometry = feature?.geometry;
    if (!geometry) {
      continue;
    }

    if (geometry.type === "LineString") {
      if (Array.isArray(geometry.coordinates)) {
        visit(geometry.coordinates, feature);
      }
    } else if (geometry.type === "MultiLineString") {
      const lines = Array.isArray(geometry.coordinates) ? geometry.coordinates : [];
      for (let l = 0; l < lines.length; l += 1) {
        if (Array.isArray(lines[l])) {
          visit(lines[l], feature);
        }
      }
    }
  }
}

function passesScaleRank(rawScaleRank, maxRank) {
  const rank = Number(rawScaleRank);
  if (!Number.isFinite(rank)) {
    return true;
  }
  return rank <= maxRank;
}

function createNormalMapFromHeightCanvas(heightCanvas, { maxSize = 2048, strength = 2.6, anisotropy = 1 }) {
  const sourceWidth = heightCanvas.width;
  const sourceHeight = heightCanvas.height;
  if (!sourceWidth || !sourceHeight) {
    return null;
  }

  const scale = Math.min(1, maxSize / sourceWidth);
  const width = Math.max(64, Math.floor(sourceWidth * scale));
  const height = Math.max(32, Math.floor(sourceHeight * scale));

  const sourceCanvas = document.createElement("canvas");
  sourceCanvas.width = width;
  sourceCanvas.height = height;
  const sourceContext = sourceCanvas.getContext("2d", { willReadFrequently: true });
  if (!sourceContext) {
    return null;
  }

  sourceContext.imageSmoothingEnabled = true;
  sourceContext.imageSmoothingQuality = "high";
  sourceContext.drawImage(heightCanvas, 0, 0, width, height);

  let sourceData;
  try {
    sourceData = sourceContext.getImageData(0, 0, width, height).data;
  } catch (error) {
    console.warn("Normal map synthesis skipped due canvas restrictions.", error);
    return null;
  }

  const outCanvas = document.createElement("canvas");
  outCanvas.width = width;
  outCanvas.height = height;
  const outContext = outCanvas.getContext("2d");
  if (!outContext) {
    return null;
  }

  const outImage = outContext.createImageData(width, height);
  const outData = outImage.data;

  const sample = (x, y) => {
    const ix = clamp(Math.round(x), 0, width - 1);
    const iy = clamp(Math.round(y), 0, height - 1);
    return sourceData[(iy * width + ix) * 4] / 255;
  };

  for (let y = 0; y < height; y += 1) {
    for (let x = 0; x < width; x += 1) {
      const left = sample(x - 1, y);
      const right = sample(x + 1, y);
      const up = sample(x, y - 1);
      const down = sample(x, y + 1);

      const dx = (left - right) * strength;
      const dy = (up - down) * strength;
      const dz = 1;
      const len = Math.hypot(dx, dy, dz) || 1;

      const i = (y * width + x) * 4;
      outData[i] = Math.round(((dx / len) * 0.5 + 0.5) * 255);
      outData[i + 1] = Math.round(((dy / len) * 0.5 + 0.5) * 255);
      outData[i + 2] = Math.round(((dz / len) * 0.5 + 0.5) * 255);
      outData[i + 3] = 255;
    }
  }

  outContext.putImageData(outImage, 0, 0);

  const normalTexture = new THREE.CanvasTexture(outCanvas);
  normalTexture.colorSpace = THREE.NoColorSpace;
  normalTexture.wrapS = THREE.RepeatWrapping;
  normalTexture.wrapT = THREE.ClampToEdgeWrapping;
  normalTexture.anisotropy = anisotropy;
  normalTexture.minFilter = THREE.LinearMipmapLinearFilter;
  normalTexture.magFilter = THREE.LinearFilter;
  normalTexture.needsUpdate = true;
  return normalTexture;
}

function createStudioEnvironment(renderer) {
  const canvas = document.createElement("canvas");
  canvas.width = 2048;
  canvas.height = 1024;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    return null;
  }

  const gradient = ctx.createLinearGradient(0, 0, 0, canvas.height);
  gradient.addColorStop(0, "#f9fbff");
  gradient.addColorStop(0.42, "#d0d7e2");
  gradient.addColorStop(0.76, "#8c97aa");
  gradient.addColorStop(1, "#626b7d");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  const addSoftbox = (cx, cy, rx, ry, color, alpha) => {
    const g = ctx.createRadialGradient(cx, cy, 0, cx, cy, Math.max(rx, ry));
    g.addColorStop(0, `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${alpha})`);
    g.addColorStop(0.32, `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${alpha * 0.55})`);
    g.addColorStop(1, "rgba(255, 255, 255, 0)");
    ctx.save();
    ctx.translate(cx, cy);
    ctx.scale(rx / Math.max(1, ry), 1);
    ctx.fillStyle = g;
    ctx.beginPath();
    ctx.arc(0, 0, Math.max(rx, ry), 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  };

  addSoftbox(canvas.width * 0.22, canvas.height * 0.24, 450, 180, [255, 255, 255], 0.3);
  addSoftbox(canvas.width * 0.78, canvas.height * 0.2, 500, 210, [245, 248, 255], 0.26);
  addSoftbox(canvas.width * 0.48, canvas.height * 0.74, 620, 240, [210, 220, 238], 0.17);

  ctx.globalAlpha = 0.16;
  ctx.fillStyle = "#ffffff";
  ctx.fillRect(0, canvas.height * 0.22, canvas.width, 26);
  ctx.fillRect(0, canvas.height * 0.64, canvas.width, 34);

  const vignette = ctx.createRadialGradient(
    canvas.width * 0.5,
    canvas.height * 0.5,
    canvas.height * 0.12,
    canvas.width * 0.5,
    canvas.height * 0.5,
    canvas.height * 0.72
  );
  vignette.addColorStop(0, "rgba(255,255,255,0)");
  vignette.addColorStop(1, "rgba(12,18,30,0.16)");
  ctx.globalAlpha = 1;
  ctx.fillStyle = vignette;
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  const source = new THREE.CanvasTexture(canvas);
  source.colorSpace = THREE.SRGBColorSpace;
  source.mapping = THREE.EquirectangularReflectionMapping;

  const pmrem = new THREE.PMREMGenerator(renderer);
  const envMap = pmrem.fromEquirectangular(source).texture;

  source.dispose();
  pmrem.dispose();
  return envMap;
}

function createStudioEnvironmentSafe(renderer) {
  try {
    return createStudioEnvironment(renderer);
  } catch (error) {
    console.warn("Environment map fallback engaged:", error);
    return null;
  }
}

function createRendererWithFallback(browser) {
  const isSafari = Boolean(browser && browser.isSafari);
  const defaultRendererCtor = THREE.WebGLRenderer;
  const webgl1RendererCtor =
    typeof THREE.WebGL1Renderer === "function" ? THREE.WebGL1Renderer : THREE.WebGLRenderer;
  const attempts = isSafari
    ? [
        {
          label: "safari-webgl-safe",
          rendererCtor: defaultRendererCtor,
          antialias: false,
          precision: "mediump",
          powerPreference: "default"
        },
        {
          label: "safari-webgl-explicit",
          rendererCtor: defaultRendererCtor,
          antialias: false,
          precision: "mediump",
          powerPreference: "default",
          explicitContext: true,
          contextTypes: ["webgl2", "webgl", "experimental-webgl"]
        },
        {
          label: "safari-webgl1-fallback",
          rendererCtor: webgl1RendererCtor,
          antialias: false,
          precision: "mediump",
          powerPreference: "default",
          explicitContext: true,
          contextTypes: ["webgl", "experimental-webgl"]
        }
      ]
    : [
        {
          label: "default-webgl-quality",
          rendererCtor: defaultRendererCtor,
          antialias: true,
          precision: "highp",
          powerPreference: "high-performance"
        },
        {
          label: "default-webgl-balanced",
          rendererCtor: defaultRendererCtor,
          antialias: false,
          precision: "highp",
          powerPreference: "high-performance"
        },
        {
          label: "default-webgl-explicit",
          rendererCtor: defaultRendererCtor,
          antialias: false,
          precision: "mediump",
          powerPreference: "default",
          explicitContext: true,
          contextTypes: ["webgl2", "webgl", "experimental-webgl"]
        },
        {
          label: "default-webgl1-fallback",
          rendererCtor: webgl1RendererCtor,
          antialias: false,
          precision: "mediump",
          powerPreference: "default",
          explicitContext: true,
          contextTypes: ["webgl", "experimental-webgl"]
        }
      ];

  const errors = [];
  for (let i = 0; i < attempts.length; i += 1) {
    const attempt = attempts[i];
    try {
      const renderer = instantiateRendererAttempt(attempt);
      renderer.domElement.dataset.rendererProfile = attempt.label;
      return renderer;
    } catch (error) {
      const reason = formatErrorMessage(error);
      errors.push(`${attempt.label}: ${reason}`);
      console.warn(`Renderer attempt failed (${attempt.label})`, error);
    }
  }

  if (errors.length) {
    console.error("Renderer initialization errors:", errors.join(" | "));
  }
  throw new Error("Error creating WebGL context.");
}

function instantiateRendererAttempt(attempt) {
  const canvas = document.createElement("canvas");
  const options = {
    canvas,
    alpha: true,
    depth: true,
    stencil: false,
    antialias: Boolean(attempt.antialias),
    premultipliedAlpha: true,
    preserveDrawingBuffer: false,
    failIfMajorPerformanceCaveat: false,
    powerPreference: attempt.powerPreference || "default",
    precision: attempt.precision || "highp"
  };
  let context = null;
  if (attempt.explicitContext) {
    context = createWebGLContext(
      canvas,
      attempt.contextTypes || ["webgl2", "webgl", "experimental-webgl"],
      {
        alpha: true,
        depth: true,
        stencil: false,
        antialias: Boolean(attempt.antialias),
        premultipliedAlpha: true,
        preserveDrawingBuffer: false,
        failIfMajorPerformanceCaveat: false,
        powerPreference: attempt.powerPreference || "default"
      }
    );
    if (!context) {
      throw new Error("No supported WebGL context available");
    }
    options.context = context;
  }

  try {
    const RendererCtor = attempt.rendererCtor || THREE.WebGLRenderer;
    const renderer = new RendererCtor(options);
    renderer.domElement.addEventListener(
      "webglcontextlost",
      (event) => {
        event.preventDefault();
        setStatus("WebGL context lost. Reload to recover.", "warn");
      },
      { passive: false }
    );
    return renderer;
  } catch (error) {
    releaseWebGLContext(context);
    throw error;
  }
}

function createWebGLContext(canvas, contextTypes, contextAttributes) {
  for (let i = 0; i < contextTypes.length; i += 1) {
    const contextType = contextTypes[i];
    try {
      const context = canvas.getContext(contextType, contextAttributes);
      if (context) {
        return context;
      }
    } catch (error) {
      console.warn(`WebGL context request failed (${contextType})`, error);
    }
  }
  return null;
}

function releaseWebGLContext(context) {
  if (!context || typeof context.getExtension !== "function") {
    return;
  }
  try {
    const extension = context.getExtension("WEBGL_lose_context");
    if (extension && typeof extension.loseContext === "function") {
      extension.loseContext();
    }
  } catch (error) {
    console.warn("Unable to release failed WebGL context", error);
  }
}

function formatErrorMessage(error) {
  if (!error) {
    return "unknown error";
  }
  if (typeof error === "string") {
    return error;
  }
  if (error.message) {
    return error.message;
  }
  return String(error);
}

function resolveLightingPresetFromQuery() {
  if (typeof window === "undefined") {
    return DEFAULT_LIGHTING_PRESET;
  }
  const params = new URLSearchParams(window.location.search);
  const requested = (params.get("lighting") || params.get("preset") || "").trim().toLowerCase();
  return requested || DEFAULT_LIGHTING_PRESET;
}

function resolveToneMappingFromQuery() {
  if (typeof window === "undefined") {
    return THREE.ACESFilmicToneMapping;
  }
  const params = new URLSearchParams(window.location.search);
  const requested = (params.get("tonemap") || "").trim().toLowerCase();
  const map = {
    aces: THREE.ACESFilmicToneMapping,
    agx: THREE.AgXToneMapping,
    cineon: THREE.CineonToneMapping,
    reinhard: THREE.ReinhardToneMapping,
    linear: THREE.LinearToneMapping,
    none: THREE.NoToneMapping
  };
  return map[requested] ?? THREE.ACESFilmicToneMapping;
}

function detectBrowserProfile() {
  if (typeof navigator === "undefined") {
    return { isSafari: false, isIOS: false, isMobile: false };
  }

  const ua = navigator.userAgent || "";
  const vendor = navigator.vendor || "";
  const isSafari =
    /Safari/i.test(ua) &&
    /Apple Computer/i.test(vendor) &&
    !/Chrome|Chromium|CriOS|Edg|OPR|FxiOS|Firefox|Android/i.test(ua);
  const isIOS =
    /iPad|iPhone|iPod/.test(ua) ||
    (navigator.platform === "MacIntel" && Number(navigator.maxTouchPoints || 0) > 1);
  const isMobile = /Mobi|Android|iPhone|iPad|iPod/i.test(ua);

  return { isSafari, isIOS, isMobile };
}

function selectRenderProfile({ browser, isCompact, memoryHint }) {
  const hasDeviceMemory = Number.isFinite(memoryHint);
  const inferredMemory = hasDeviceMemory ? memoryHint : 12;
  const lowMemory = inferredMemory <= 4;
  const midMemory = inferredMemory <= 8;
  const textureWidth = lowMemory ? 4096 : (isCompact || midMemory) ? 8192 : 16384;

  return {
    textureWidth,
    maxPixelRatio: textureWidth >= 16384 ? 2.1 : 2.35,
    countriesSources: ASSETS.countriesHigh,
    statesSources: ASSETS.statesHigh,
    riversSources: textureWidth >= 8192 ? ASSETS.riversHigh : ASSETS.riversCompatible,
    lakesSources: textureWidth >= 8192 ? ASSETS.lakesHigh : ASSETS.lakesCompatible,
    borderCountryStepDeg: textureWidth >= 16384 ? 0.34 : textureWidth >= 8192 ? 0.55 : 1.05,
    borderStateStepDeg: textureWidth >= 16384 ? 0.46 : textureWidth >= 8192 ? 0.72 : 1.3,
    hydroRiverStepDeg: textureWidth >= 16384 ? 0.44 : textureWidth >= 8192 ? 0.66 : 1.2,
    hydroLakeStepDeg: textureWidth >= 16384 ? 0.5 : textureWidth >= 8192 ? 0.74 : 1.3,
    geoTimeoutMs: textureWidth >= 16384 ? 26000 : 22000,
    stateTimeoutMs: textureWidth >= 16384 ? 90000 : 65000
  };
}

function resolveSupportedTextureWidth(requestedWidth, maxTextureSize) {
  const cap = Math.max(1024, Number(maxTextureSize) || 8192);
  const requested = Math.max(1024, Number(requestedWidth) || 4096);
  const limit = Math.min(cap, requested);
  const ladder = [16384, 12288, 8192, 6144, 4096, 3072, 2048, 1024];
  for (let i = 0; i < ladder.length; i += 1) {
    if (ladder[i] <= limit) {
      return ladder[i];
    }
  }
  return 1024;
}

function resolveHeightMapSources(textureWidth) {
  if (textureWidth >= 16384) {
    return ASSETS.heightMapsUltra;
  }
  if (textureWidth >= 8192) {
    return ASSETS.heightMapsHigh;
  }
  return ASSETS.heightMapsCompatible;
}

function resolveMeshSegments(textureWidth) {
  if (textureWidth >= 16384) {
    return { width: 2688, height: 1344, shellWidth: 214, shellHeight: 148 };
  }
  if (textureWidth >= 8192) {
    return { width: 2048, height: 1180, shellWidth: 188, shellHeight: 132 };
  }
  if (textureWidth >= 4096) {
    return { width: 1536, height: 896, shellWidth: 164, shellHeight: 116 };
  }
  if (textureWidth >= 3072) {
    return { width: 1180, height: 720, shellWidth: 148, shellHeight: 104 };
  }
  return { width: 960, height: 620, shellWidth: 132, shellHeight: 94 };
}

function getImageIntrinsicSize(image) {
  if (!image) {
    return { width: 0, height: 0 };
  }
  const width = Number(image.naturalWidth || image.videoWidth || image.width || 0);
  const height = Number(image.naturalHeight || image.videoHeight || image.height || 0);
  return {
    width: Number.isFinite(width) ? width : 0,
    height: Number.isFinite(height) ? height : 0
  };
}

function applySurfaceSettings(globeMaterial, shellMaterial, materialSettings) {
  if (globeMaterial) {
    globeMaterial.roughness = clamp(materialSettings.roughness, 0.3, 1);
    globeMaterial.clearcoat = clamp(materialSettings.clearcoat, 0, 0.45);
    globeMaterial.clearcoatRoughness = clamp(materialSettings.clearcoatRoughness, 0.2, 1);
    globeMaterial.envMapIntensity = clamp(materialSettings.envMapIntensity, 0, 1.2);
    if ("ior" in globeMaterial) {
      globeMaterial.ior = clamp(materialSettings.ior, 1, 2.333);
    }
    if ("reflectivity" in globeMaterial) {
      globeMaterial.reflectivity = clamp(materialSettings.reflectivity, 0, 1);
    }
    if ("specularIntensity" in globeMaterial) {
      globeMaterial.specularIntensity = clamp(materialSettings.specularIntensity, 0, 2);
    }
    if ("sheen" in globeMaterial) {
      globeMaterial.sheen = clamp(materialSettings.sheen, 0, 1);
    }
    if ("sheenRoughness" in globeMaterial) {
      globeMaterial.sheenRoughness = clamp(materialSettings.sheenRoughness, 0, 1);
    }
    if ("sheenColor" in globeMaterial) {
      globeMaterial.sheenColor = new THREE.Color(materialSettings.sheenColor);
    }
    const terrainExaggeration = clamp(
      Number(materialSettings.terrainExaggeration) || 1,
      TERRAIN_EXAGGERATION_MIN,
      TERRAIN_EXAGGERATION_MAX
    );
    const normalStrength = clamp(Number(materialSettings.normalStrength) || 1, 0.35, 2.5);
    const reliefUpdater = globeMaterial.userData?.updateRelief;
    const reliefState = globeMaterial.userData?.reliefState || {};
    const reliefChanged =
      !Number.isFinite(reliefState.terrainExaggeration) ||
      !Number.isFinite(reliefState.normalStrength) ||
      Math.abs(reliefState.terrainExaggeration - terrainExaggeration) > 1e-4 ||
      Math.abs(reliefState.normalStrength - normalStrength) > 1e-4;
    if (typeof reliefUpdater === "function" && reliefChanged) {
      reliefUpdater(terrainExaggeration, normalStrength);
      globeMaterial.userData.reliefState = {
        terrainExaggeration,
        normalStrength
      };
    }
    globeMaterial.needsUpdate = true;
  }

  if (shellMaterial) {
    shellMaterial.opacity = clamp(materialSettings.shellIntensity, 0, 0.2);
    shellMaterial.needsUpdate = true;
  }
}

function mix(a, b, t) {
  return a + (b - a) * t;
}

function formatNumber2(value) {
  return Number(value).toFixed(2);
}

function formatDegrees(value) {
  return `${Math.round(Number(value))}deg`;
}

function formatKelvin(value) {
  return `${Math.round(Number(value))}K`;
}

function formatPresetName(presetName) {
  return String(presetName)
    .split("-")
    .map((part) => (part.length ? part[0].toUpperCase() + part.slice(1) : part))
    .join(" ");
}

function setColorFromKelvin(color, kelvin) {
  const temp = clamp(Number(kelvin) || 6500, 1000, 40000) / 100;
  let red;
  let green;
  let blue;

  if (temp <= 66) {
    red = 255;
    green = 99.4708025861 * Math.log(Math.max(1, temp)) - 161.1195681661;
    if (temp <= 19) {
      blue = 0;
    } else {
      blue = 138.5177312231 * Math.log(temp - 10) - 305.0447927307;
    }
  } else {
    red = 329.698727446 * Math.pow(temp - 60, -0.1332047592);
    green = 288.1221695283 * Math.pow(temp - 60, -0.0755148492);
    blue = 255;
  }

  color.setRGB(
    clamp(red / 255, 0, 1),
    clamp(green / 255, 0, 1),
    clamp(blue / 255, 0, 1)
  );
}

function smoothstep(edge0, edge1, x) {
  const t = clamp((x - edge0) / Math.max(1e-6, edge1 - edge0), 0, 1);
  return t * t * (3 - 2 * t);
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function clamp01(value) {
  return clamp(value, 0, 1);
}
