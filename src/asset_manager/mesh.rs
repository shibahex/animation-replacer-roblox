//! Converts Roblox `.mesh` files to `.glb`, so meshes can be reuploaded through Open Cloud
//! (which only accepts 3D files like `.fbx`/`.glb` for meshes, uploaded as a Model).
//!
//! Supports mesh versions 1.00 to 5.00. Only the most detailed level of detail is kept.
//! Bones and skinning are kept, FACS (face animation) and vertex colors are dropped.

use anyhow::{Context, bail};

/// One vertex of a mesh, in Roblox units (studs).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vertex {
    pub position: [f32; 3],
    pub normal: [f32; 3],
    pub uv: [f32; 2],
}

/// A bone of a skinned mesh. The transform is in mesh space, not relative to the parent.
#[derive(Debug, Clone, PartialEq)]
pub struct Bone {
    pub name: String,
    pub parent: Option<usize>,
    /// Rotation matrix, row by row (like a Roblox CFrame's R00..R22).
    pub rotation: [f32; 9],
    pub position: [f32; 3],
}

/// Which bones move a vertex, and how much. Indices point into `Mesh::bones`.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct VertexSkin {
    pub bones: [u16; 4],
    pub weights: [f32; 4],
}

/// The geometry of a mesh: vertices and triangles (indices into `vertices`).
#[derive(Debug, Clone, Default)]
pub struct Mesh {
    pub vertices: Vec<Vertex>,
    pub triangles: Vec<[u32; 3]>,
    /// Bones of a skinned mesh, empty for static meshes.
    pub bones: Vec<Bone>,
    /// One entry per vertex for skinned meshes, empty for static meshes.
    pub skin: Vec<VertexSkin>,
}

impl Mesh {
    /// Returns true if the mesh is skinned (has bones).
    pub fn has_bones(&self) -> bool {
        !self.bones.is_empty()
    }

    /// The center of the mesh's bounding box. Roblox moves uploaded meshes so this is at 0.
    pub fn center(&self) -> [f32; 3] {
        let mut min = [f32::MAX; 3];
        let mut max = [f32::MIN; 3];
        for vertex in &self.vertices {
            for axis in 0..3 {
                min[axis] = min[axis].min(vertex.position[axis]);
                max[axis] = max[axis].max(vertex.position[axis]);
            }
        }
        [0, 1, 2].map(|axis| (min[axis] + max[axis]) / 2.0)
    }
}

/// Reads a Roblox `.mesh` file (version 1.00 to 5.00).
pub fn read_mesh(data: &[u8]) -> anyhow::Result<Mesh> {
    let newline = data
        .iter()
        .position(|&b| b == b'\n')
        .context("missing mesh version line")?;
    let version = std::str::from_utf8(&data[..newline])
        .context("invalid mesh version line")?
        .trim()
        .trim_start_matches("version ")
        .to_string();
    let body = &data[newline + 1..];

    match version.as_str() {
        "1.00" | "1.01" => read_v1(body, version == "1.00"),
        "2.00" => read_v2(body),
        "3.00" | "3.01" => read_v3(body),
        "4.00" | "4.01" | "5.00" => read_v4(body),
        other => bail!("unsupported mesh version {}", other),
    }
}

/// Version 1 is text: the face count, then 3 `[x,y,z][nx,ny,nz][u,v,w]` vertices per face.
/// Version 1.00 meshes are stored at twice their size, and their V coordinate is flipped.
fn read_v1(body: &[u8], is_1_00: bool) -> anyhow::Result<Mesh> {
    let text = std::str::from_utf8(body).context("version 1 mesh isn't text")?;
    let mut lines = text.lines();
    let face_count: usize = lines
        .next()
        .context("missing face count")?
        .trim()
        .parse()
        .context("invalid face count")?;

    let numbers: Vec<f32> = lines
        .flat_map(|line| line.split(['[', ']', ',']))
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| part.parse::<f32>())
        .collect::<Result<_, _>>()
        .context("invalid number in version 1 mesh")?;

    let vertex_count = face_count * 3;
    if numbers.len() < vertex_count * 9 {
        bail!("version 1 mesh is shorter than its face count");
    }

    let scale = if is_1_00 { 0.5 } else { 1.0 };
    let vertices = numbers
        .chunks_exact(9)
        .take(vertex_count)
        .map(|n| Vertex {
            position: [n[0] * scale, n[1] * scale, n[2] * scale],
            normal: [n[3], n[4], n[5]],
            uv: [n[6], 1.0 - n[7]],
        })
        .collect();
    let triangles = (0..face_count as u32)
        .map(|face| [face * 3, face * 3 + 1, face * 3 + 2])
        .collect();

    Ok(Mesh {
        vertices,
        triangles,
        ..Default::default()
    })
}

/// Version 2: a small header, then binary vertices and faces.
fn read_v2(body: &[u8]) -> anyhow::Result<Mesh> {
    let mut reader = Reader::new(body);
    let header_size = reader.u16()? as usize;
    let vertex_size = reader.u8()? as usize;
    let _face_size = reader.u8()?;
    let vertex_count = reader.u32()? as usize;
    let face_count = reader.u32()? as usize;
    reader.seek(header_size)?;

    let vertices = reader.vertices(vertex_count, vertex_size)?;
    let triangles = reader.faces(face_count)?;
    finish(vertices, triangles)
}

/// Version 3: like version 2, plus levels of detail after the faces.
fn read_v3(body: &[u8]) -> anyhow::Result<Mesh> {
    let mut reader = Reader::new(body);
    let header_size = reader.u16()? as usize;
    let vertex_size = reader.u8()? as usize;
    let _face_size = reader.u8()?;
    let _lod_size = reader.u16()?;
    let lod_count = reader.u16()? as usize;
    let vertex_count = reader.u32()? as usize;
    let face_count = reader.u32()? as usize;
    reader.seek(header_size)?;

    let vertices = reader.vertices(vertex_count, vertex_size)?;
    let faces = reader.faces(face_count)?;
    let lods = reader.lods(lod_count)?;
    finish(vertices, highest_lod(faces, &lods))
}

/// Versions 4 and 5: adds bones, skinning and subsets (and FACS in version 5, which is skipped).
fn read_v4(body: &[u8]) -> anyhow::Result<Mesh> {
    let mut reader = Reader::new(body);
    let header_size = reader.u16()? as usize;
    let _lod_type = reader.u16()?;
    let vertex_count = reader.u32()? as usize;
    let face_count = reader.u32()? as usize;
    let lod_count = reader.u16()? as usize;
    let bone_count = reader.u16()? as usize;
    let bone_names_size = reader.u32()? as usize;
    let subset_count = reader.u16()? as usize;
    reader.seek(header_size)?;

    // Vertices are always 40 bytes in version 4 and 5
    let vertices = reader.vertices(vertex_count, 40)?;

    // Skinning: 4 bone indices (into the vertex's subset) and 4 weights (0-255) per vertex
    let mut envelopes = Vec::new();
    if bone_count > 0 {
        for _ in 0..vertex_count {
            let bones: [u8; 4] = reader.take(4)?.try_into()?;
            let weights: [u8; 4] = reader.take(4)?.try_into()?;
            envelopes.push((bones, weights));
        }
    }

    let faces = reader.faces(face_count)?;
    let lods = reader.lods(lod_count)?;

    let mut raw_bones = Vec::with_capacity(bone_count);
    for _ in 0..bone_count {
        let name_offset = reader.u32()? as usize;
        let parent = reader.u16()?;
        let _lod_parent = reader.u16()?;
        let _culling = reader.f32()?;
        let mut rotation = [0.0; 9];
        for value in &mut rotation {
            *value = reader.f32()?;
        }
        let position = [reader.f32()?, reader.f32()?, reader.f32()?];
        raw_bones.push((name_offset, parent, rotation, position));
    }

    let names = reader.take(bone_names_size)?;
    let bones: Vec<Bone> = raw_bones
        .into_iter()
        .map(|(name_offset, parent, rotation, position)| Bone {
            name: names
                .get(name_offset..)
                .and_then(|rest| rest.split(|&b| b == 0).next())
                .map(|name| String::from_utf8_lossy(name).into_owned())
                .unwrap_or_default(),
            parent: (parent != u16::MAX && (parent as usize) < bone_count)
                .then_some(parent as usize),
            rotation,
            position,
        })
        .collect();

    // Subsets map a vertex's local bone indices to the mesh's bone list
    let mut subsets = Vec::with_capacity(subset_count);
    for _ in 0..subset_count {
        let _faces_begin = reader.u32()?;
        let _faces_length = reader.u32()?;
        let verts_begin = reader.u32()? as usize;
        let verts_length = reader.u32()? as usize;
        let bone_index_count = (reader.u32()? as usize).min(26);
        let mut bone_indices = [0_u16; 26];
        for index in &mut bone_indices {
            *index = reader.u16()?;
        }
        subsets.push((
            verts_begin..verts_begin + verts_length,
            bone_index_count,
            bone_indices,
        ));
    }

    let skin = envelopes
        .iter()
        .enumerate()
        .map(|(vertex, (local_bones, weights))| {
            let subset_bones = subsets
                .iter()
                .find(|(range, _, _)| range.contains(&vertex))
                .map(|(_, count, indices)| &indices[..*count]);
            vertex_skin(*local_bones, *weights, subset_bones, bones.len())
        })
        .collect();

    let mut mesh = finish(vertices, highest_lod(faces, &lods))?;
    mesh.bones = bones;
    mesh.skin = skin;
    Ok(mesh)
}

/// Turns a vertex's subset bone indices and 0-255 weights into mesh bone indices and weights
/// that add up to 1. A vertex without weights follows the first bone.
fn vertex_skin(
    local_bones: [u8; 4],
    weights: [u8; 4],
    subset_bones: Option<&[u16]>,
    bone_count: usize,
) -> VertexSkin {
    let mut skin = VertexSkin::default();
    let mut total = 0.0;

    for slot in 0..4 {
        let bone = subset_bones
            .and_then(|indices| indices.get(local_bones[slot] as usize))
            .copied()
            .filter(|&bone| (bone as usize) < bone_count);

        if let Some(bone) = bone
            && weights[slot] > 0
        {
            skin.bones[slot] = bone;
            skin.weights[slot] = weights[slot] as f32;
            total += weights[slot] as f32;
        }
    }

    if total > 0.0 {
        for weight in &mut skin.weights {
            *weight /= total;
        }
        skin
    } else {
        VertexSkin {
            bones: [0; 4],
            weights: [1.0, 0.0, 0.0, 0.0],
        }
    }
}

/// Keeps only the faces of the most detailed level of detail (the first one).
fn highest_lod(faces: Vec<[u32; 3]>, lods: &[u32]) -> Vec<[u32; 3]> {
    match lods {
        [start, end, ..] if start < end && (*end as usize) <= faces.len() => {
            faces[*start as usize..*end as usize].to_vec()
        }
        _ => faces,
    }
}

/// Drops faces that point outside the vertex list, so a damaged mesh can't produce a broken file.
fn finish(vertices: Vec<Vertex>, triangles: Vec<[u32; 3]>) -> anyhow::Result<Mesh> {
    let vertex_count = vertices.len() as u32;
    let triangles: Vec<_> = triangles
        .into_iter()
        .filter(|face| face.iter().all(|&i| i < vertex_count))
        .collect();

    if triangles.is_empty() {
        bail!("mesh has no faces");
    }
    Ok(Mesh {
        vertices,
        triangles,
        ..Default::default()
    })
}

/// Reads little-endian values from a mesh file.
struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn take(&mut self, len: usize) -> anyhow::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .filter(|&end| end <= self.data.len())
            .context("mesh file ends early")?;
        let bytes = &self.data[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn seek(&mut self, position: usize) -> anyhow::Result<()> {
        if position > self.data.len() {
            bail!("mesh file ends early");
        }
        self.position = position;
        Ok(())
    }

    fn u8(&mut self) -> anyhow::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> anyhow::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into()?))
    }

    fn u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }

    fn f32(&mut self) -> anyhow::Result<f32> {
        Ok(f32::from_le_bytes(self.take(4)?.try_into()?))
    }

    /// Reads vertices: position, normal and UV, then skips the rest (tangent, color).
    fn vertices(&mut self, count: usize, size: usize) -> anyhow::Result<Vec<Vertex>> {
        if size < 32 {
            bail!("unsupported vertex size {}", size);
        }

        (0..count)
            .map(|_| {
                let start = self.position;
                let vertex = Vertex {
                    position: [self.f32()?, self.f32()?, self.f32()?],
                    normal: [self.f32()?, self.f32()?, self.f32()?],
                    uv: [self.f32()?, self.f32()?],
                };
                self.seek(start + size)?;
                Ok(vertex)
            })
            .collect()
    }

    fn faces(&mut self, count: usize) -> anyhow::Result<Vec<[u32; 3]>> {
        (0..count)
            .map(|_| Ok([self.u32()?, self.u32()?, self.u32()?]))
            .collect()
    }

    fn lods(&mut self, count: usize) -> anyhow::Result<Vec<u32>> {
        (0..count).map(|_| self.u32()).collect()
    }
}

/// A 4x4 transform, stored column by column like glTF expects.
type Matrix = [f32; 16];

/// A bone's mesh space transform as a matrix.
fn bone_matrix(bone: &Bone) -> Matrix {
    let r = bone.rotation;
    let p = bone.position;
    [
        r[0], r[3], r[6], 0.0, //
        r[1], r[4], r[7], 0.0, //
        r[2], r[5], r[8], 0.0, //
        p[0], p[1], p[2], 1.0,
    ]
}

/// Multiplies two column-major matrices (`a * b`).
fn multiply(a: &Matrix, b: &Matrix) -> Matrix {
    let mut out = [0.0; 16];
    for column in 0..4 {
        for row in 0..4 {
            out[column * 4 + row] = (0..4).map(|k| a[k * 4 + row] * b[column * 4 + k]).sum();
        }
    }
    out
}

/// Inverts a transform made of a 3x3 part and a translation. Returns the identity if it
/// can't be inverted, so a broken bone can't make an invalid file.
fn invert(m: &Matrix) -> Matrix {
    let (a, b, c) = (m[0], m[4], m[8]);
    let (d, e, f) = (m[1], m[5], m[9]);
    let (g, h, i) = (m[2], m[6], m[10]);
    let det = a * (e * i - f * h) - b * (d * i - f * g) + c * (d * h - e * g);
    if !det.is_finite() || det.abs() < 1e-8 {
        return identity();
    }

    // Rows of the inverted 3x3 part
    let inv = [
        [
            (e * i - f * h) / det,
            (c * h - b * i) / det,
            (b * f - c * e) / det,
        ],
        [
            (f * g - d * i) / det,
            (a * i - c * g) / det,
            (c * d - a * f) / det,
        ],
        [
            (d * h - e * g) / det,
            (b * g - a * h) / det,
            (a * e - b * d) / det,
        ],
    ];
    let t = [m[12], m[13], m[14]];
    let new_t =
        [0, 1, 2].map(|row| -(inv[row][0] * t[0] + inv[row][1] * t[1] + inv[row][2] * t[2]));

    [
        inv[0][0], inv[1][0], inv[2][0], 0.0, //
        inv[0][1], inv[1][1], inv[2][1], 0.0, //
        inv[0][2], inv[1][2], inv[2][2], 0.0, //
        new_t[0], new_t[1], new_t[2], 1.0,
    ]
}

fn identity() -> Matrix {
    let mut m = [0.0; 16];
    m[0] = 1.0;
    m[5] = 1.0;
    m[10] = 1.0;
    m[15] = 1.0;
    m
}

/// Adds a buffer view and accessor for `data`, returning the accessor's index.
struct GlbBuilder {
    buffer: Vec<u8>,
    views: Vec<serde_json::Value>,
    accessors: Vec<serde_json::Value>,
}

impl GlbBuilder {
    fn add(&mut self, data: Vec<u8>, accessor: serde_json::Value, target: Option<u32>) -> usize {
        let mut view = serde_json::json!({
            "buffer": 0,
            "byteOffset": self.buffer.len(),
            "byteLength": data.len(),
        });
        if let Some(target) = target {
            view["target"] = serde_json::json!(target);
        }

        let mut accessor = accessor;
        accessor["bufferView"] = serde_json::json!(self.views.len());
        self.views.push(view);
        self.buffer.extend(data);
        self.accessors.push(accessor);
        self.accessors.len() - 1
    }
}

const ARRAY_BUFFER: Option<u32> = Some(34962);
const ELEMENT_ARRAY_BUFFER: Option<u32> = Some(34963);
const FLOAT: u32 = 5126;
const UNSIGNED_SHORT: u32 = 5123;
const UNSIGNED_INT: u32 = 5125;

/// Turns a mesh 180 degrees around Y: `(x, y, z)` becomes `(-x, y, -z)`.
///
/// Roblox's importer turns glTF files 180 degrees (glTF faces +Z, Roblox faces -Z), so meshes
/// are turned the other way first to come out the same as the original.
fn turn_around(mesh: &Mesh) -> Mesh {
    let turn = |v: [f32; 3]| [-v[0], v[1], -v[2]];
    let mut turned = mesh.clone();

    for vertex in &mut turned.vertices {
        vertex.position = turn(vertex.position);
        vertex.normal = turn(vertex.normal);
    }
    for bone in &mut turned.bones {
        // Rotation R becomes T * R * T, with T = diag(-1, 1, -1)
        let r = bone.rotation;
        bone.rotation = [r[0], -r[1], r[2], -r[3], r[4], -r[5], r[6], -r[7], r[8]];
        bone.position = turn(bone.position);
    }
    turned
}

/// Writes the mesh as a binary glTF (`.glb`) file, ready for Roblox's importer. `name` becomes
/// the name of the mesh. Skinned meshes get their bones as a glTF skin, so Roblox imports them
/// as a skinned mesh with the same bone names.
pub fn write_glb(mesh: &Mesh, name: &str) -> Vec<u8> {
    let mesh = &turn_around(mesh);
    let vertex_count = mesh.vertices.len();
    let floats = |values: &mut dyn Iterator<Item = f32>| -> Vec<u8> {
        values.flat_map(f32::to_le_bytes).collect()
    };

    let mut min = [f32::MAX; 3];
    let mut max = [f32::MIN; 3];
    for vertex in &mesh.vertices {
        for axis in 0..3 {
            min[axis] = min[axis].min(vertex.position[axis]);
            max[axis] = max[axis].max(vertex.position[axis]);
        }
    }

    let mut glb = GlbBuilder {
        buffer: Vec::new(),
        views: Vec::new(),
        accessors: Vec::new(),
    };

    let positions = glb.add(
        floats(&mut mesh.vertices.iter().flat_map(|v| v.position)),
        serde_json::json!({ "componentType": FLOAT, "count": vertex_count, "type": "VEC3", "min": min, "max": max }),
        ARRAY_BUFFER,
    );
    let normals = glb.add(
        floats(&mut mesh.vertices.iter().flat_map(|v| unit_normal(v.normal))),
        serde_json::json!({ "componentType": FLOAT, "count": vertex_count, "type": "VEC3" }),
        ARRAY_BUFFER,
    );
    let uvs = glb.add(
        floats(&mut mesh.vertices.iter().flat_map(|v| v.uv)),
        serde_json::json!({ "componentType": FLOAT, "count": vertex_count, "type": "VEC2" }),
        ARRAY_BUFFER,
    );
    let indices = glb.add(
        mesh.triangles.iter().flatten().flat_map(|i| i.to_le_bytes()).collect(),
        serde_json::json!({ "componentType": UNSIGNED_INT, "count": mesh.triangles.len() * 3, "type": "SCALAR" }),
        ELEMENT_ARRAY_BUFFER,
    );

    let mut attributes =
        serde_json::json!({ "POSITION": positions, "NORMAL": normals, "TEXCOORD_0": uvs });
    let mut nodes = vec![serde_json::json!({ "mesh": 0, "name": name })];
    let mut scene_nodes = vec![0];
    let mut skins = Vec::new();

    let skinned = mesh.has_bones() && mesh.skin.len() == vertex_count;
    if skinned {
        let joints = glb.add(
            mesh.skin.iter().flat_map(|s| s.bones).flat_map(u16::to_le_bytes).collect(),
            serde_json::json!({ "componentType": UNSIGNED_SHORT, "count": vertex_count, "type": "VEC4" }),
            ARRAY_BUFFER,
        );
        let weights = glb.add(
            floats(&mut mesh.skin.iter().flat_map(|s| s.weights)),
            serde_json::json!({ "componentType": FLOAT, "count": vertex_count, "type": "VEC4" }),
            ARRAY_BUFFER,
        );
        attributes["JOINTS_0"] = serde_json::json!(joints);
        attributes["WEIGHTS_0"] = serde_json::json!(weights);

        let world: Vec<Matrix> = mesh.bones.iter().map(bone_matrix).collect();
        let inverse_binds = glb.add(
            floats(&mut world.iter().flat_map(invert)),
            serde_json::json!({ "componentType": FLOAT, "count": mesh.bones.len(), "type": "MAT4" }),
            None,
        );

        // Bone nodes come after the mesh node, so bone i is node i + 1
        for (index, bone) in mesh.bones.iter().enumerate() {
            let local = match bone.parent {
                Some(parent) => multiply(&invert(&world[parent]), &world[index]),
                None => world[index],
            };
            let children: Vec<usize> = mesh
                .bones
                .iter()
                .enumerate()
                .filter(|(_, child)| child.parent == Some(index))
                .map(|(child, _)| child + 1)
                .collect();

            let mut node = serde_json::json!({ "name": bone.name, "matrix": local });
            if !children.is_empty() {
                node["children"] = serde_json::json!(children);
            }
            nodes.push(node);
            if bone.parent.is_none() {
                scene_nodes.push(index + 1);
            }
        }

        nodes[0]["skin"] = serde_json::json!(0);
        skins.push(serde_json::json!({
            "joints": (1..=mesh.bones.len()).collect::<Vec<_>>(),
            "inverseBindMatrices": inverse_binds,
        }));
    }

    let mut json = serde_json::json!({
        "asset": { "version": "2.0", "generator": "animation-replacer-roblox" },
        "scene": 0,
        "scenes": [{ "nodes": scene_nodes }],
        "nodes": nodes,
        "meshes": [{
            "name": name,
            "primitives": [{ "attributes": attributes, "indices": indices, "mode": 4 }],
        }],
        "buffers": [{ "byteLength": glb.buffer.len() }],
        "bufferViews": glb.views,
        "accessors": glb.accessors,
    });
    if !skins.is_empty() {
        json["skins"] = serde_json::json!(skins);
    }

    let mut json = serde_json::to_vec(&json).expect("glTF json is always valid");
    while !json.len().is_multiple_of(4) {
        json.push(b' ');
    }
    let mut buffer = glb.buffer;
    while !buffer.len().is_multiple_of(4) {
        buffer.push(0);
    }

    let total_len = 12 + 8 + json.len() + 8 + buffer.len();
    let mut out = Vec::with_capacity(total_len);
    out.extend(b"glTF");
    out.extend(2_u32.to_le_bytes());
    out.extend((total_len as u32).to_le_bytes());
    out.extend((json.len() as u32).to_le_bytes());
    out.extend(b"JSON");
    out.extend(json);
    out.extend((buffer.len() as u32).to_le_bytes());
    out.extend(b"BIN\0");
    out.extend(buffer);
    out
}

/// glTF needs unit length normals. Broken (zero) normals point up instead.
fn unit_normal(normal: [f32; 3]) -> [f32; 3] {
    let length = (normal[0] * normal[0] + normal[1] * normal[1] + normal[2] * normal[2]).sqrt();
    if length.is_finite() && length > 1e-6 {
        [normal[0] / length, normal[1] / length, normal[2] / length]
    } else {
        [0.0, 1.0, 0.0]
    }
}

#[cfg(test)]
mod tests {
    use super::{read_mesh, write_glb};

    /// A version 2.00 mesh with one triangle.
    fn v2_triangle() -> Vec<u8> {
        let mut data = b"version 2.00\n".to_vec();
        data.extend(12_u16.to_le_bytes());
        data.push(40);
        data.push(12);
        data.extend(3_u32.to_le_bytes());
        data.extend(1_u32.to_le_bytes());
        for (x, y) in [(0.0_f32, 0.0_f32), (1.0, 0.0), (0.0, 1.0)] {
            for value in [x, y, 0.0, 0.0, 0.0, 1.0, x, y] {
                data.extend(value.to_le_bytes());
            }
            data.extend([0_u8; 8]);
        }
        for index in [0_u32, 1, 2] {
            data.extend(index.to_le_bytes());
        }
        data
    }

    #[test]
    fn reads_version_2_meshes() {
        let mesh = read_mesh(&v2_triangle()).unwrap();
        assert_eq!(mesh.vertices.len(), 3);
        assert_eq!(mesh.triangles, vec![[0, 1, 2]]);
        assert_eq!(mesh.vertices[1].position, [1.0, 0.0, 0.0]);
        assert_eq!(mesh.vertices[2].uv, [0.0, 1.0]);
    }

    #[test]
    fn reads_version_1_meshes() {
        let data =
            b"version 1.00\n1\n[0,0,0][0,0,1][0,0,0][2,0,0][0,0,1][1,0,0][0,2,0][0,0,1][0,1,0]";
        let mesh = read_mesh(data).unwrap();
        assert_eq!(mesh.triangles, vec![[0, 1, 2]]);
        // Version 1.00 is stored at twice the size, and its V is flipped
        assert_eq!(mesh.vertices[1].position, [1.0, 0.0, 0.0]);
        assert_eq!(mesh.vertices[2].uv, [0.0, 0.0]);
    }

    #[test]
    fn rejects_broken_meshes() {
        assert!(read_mesh(b"version 9.00\n").is_err());
        let mut short = v2_triangle();
        short.truncate(40);
        assert!(read_mesh(&short).is_err());
    }

    #[test]
    fn inverts_bone_matrices() {
        use super::{Bone, bone_matrix, identity, invert, multiply};
        let bone = Bone {
            name: "Root".into(),
            parent: None,
            // 90 degrees around Y, moved up
            rotation: [0.0, 0.0, 1.0, 0.0, 1.0, 0.0, -1.0, 0.0, 0.0],
            position: [1.0, 2.0, 3.0],
        };
        let m = bone_matrix(&bone);
        let product = multiply(&m, &invert(&m));
        for (a, b) in product.iter().zip(identity().iter()) {
            assert!((a - b).abs() < 1e-5);
        }
    }

    #[test]
    fn writes_a_valid_glb_header() {
        let glb = write_glb(&read_mesh(&v2_triangle()).unwrap(), "Triangle");
        assert!(glb.starts_with(b"glTF"));
        assert_eq!(
            u32::from_le_bytes(glb[8..12].try_into().unwrap()) as usize,
            glb.len()
        );
        assert!(glb.len().is_multiple_of(4));
    }
}
