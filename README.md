# 🎭 Animation Replacer for Roblox (WIP)
[![Rust](https://img.shields.io/badge/rust-%23000000.svg?style=for-the-badge&logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![Roblox](https://img.shields.io/badge/Roblox-00A2FF?style=for-the-badge&logo=roblox&logoColor=white)](https://www.roblox.com/)

**Animation Transfering/Spoofing** is an automated process designed to fix the handling of third party animations in your Roblox game. This program will scrape Roblox files, spoof/reupload animations, and republish them to ensure animation transfer when you publish your game.
<div align="center">

[![Showcase Video](https://img.youtube.com/vi/URB9llET-w8/0.jpg)](https://www.youtube.com/watch?v=URB9llET-w8 "Showcase Video")
<footer>Video Showcase</footer>
</div>

>    [!WARNING]
>    This program has been unstable due to the lack of updates after roblox's changes.
>    Major bug on fetching animations will be resolved soon, then will try to make it stable again
>    
> Report issues here: https://github.com/shibahex/animation-replacer-roblox/issues

## 📝 Development Status
- ✅ Scrape animations in lua scripts
- ✅ Scrape animation objects in the game file
- ✅ Fetch animation metadata, file contents, and asset types
- ✅ Upload multiple animations in a concurrent system; using [semaphore](https://docs.rs/semaphore/latest/semaphore/)
- ✅ Writing animations back to script source 
- ✅ Flags and user configuration for easy use
- ✅ Replace the animation instances in-game (Only replaces scripts for now)  
- ❌ Rename the Animations as the same as the ones it replaces (Requires extra API calls for scripts)

## 📦 Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/animation-replacer-roblox.git
   cd animation-replacer-roblox
   ```

2. **Build the project**
   ```bash
   cargo build --release
   ```
> 🪟 For Windows users heres: [cargo install guide](https://doc.rust-lang.org/cargo/getting-started/installation.html) (to fix cargo not working)

3. **Run the application**

To run the tool, you’ll need your Roblox ``.ROBLOSECURITY`` cookie.
This is required to authenticate your account for uploading animations.

>    [!WARNING]
>    Never share your Roblox cookie. It grants full access to your account.
>    If you're unsure how to retrieve it, here’s a tutorial:
>    [How to get your Roblox cookie (YouTube)](https://www.youtube.com/watch?v=zkSnBV7oOZM)
> 
> If you're concerned about using your main account, consider creating an alternate account, adding it as an admin to your group, and uploading from there.

You will also need to open Roblox Studio and save the game as a file for the Animation Replacer.
> [!NOTE]
> Recommended to use the ``--output`` flag to avoid data loss if the game corrupts. 

   ```bash
   cargo run -- --cookie "COOKIEHERE" --file "example.rbxl" --output "output.rbxl"
   ```

<div align="center">
⚠️ Animations won't function in games owned by a group ⚠️
</div align="center">

> [!NOTE]
> If you're uploading the game through a group, be sure to include the ``--group "GROUP_ID"`` flag.
>
> make sure the account has a LEGACY role with permissions to manage and create assets to avoid Bad Request

<!-- > ⚠️ This project is currently under active development.   -->
<!-- > Installation instructions will be provided in a future release. ⚠️ -->
---

## 💻 How It Works
1. **File Scanning** - The bot automatically scans and identifies animations in the Roblox files
2. **Reuploading** - Each animation is processed and republished to ensure compatbility
3. **Completion** - Your game has working animations.

## ⚙️ Configuration
The tool requires minimal setup:
- **Roblox Cookie (REQUIRED)**: Your authentication token for accessing Roblox services
- **Target File (REQUIRED)**: --file requires the path of the file to scan
- **Group id (Optional)**: Upload to a group with --group flag
- **Output (Optional)**: Use the --output flag to avoid data loss
- **Threads (Optional)**: the --threads flag is how many concurrent tasks need to run (default is 5)

## 🚨 Important Notes

### Disclaimer
- Users are responsible for compliance with Roblox's policies
- Always ask/give credit to animators.
- Your Roblox cookie is only used for authentication purposes


## 🤝 Credit 
Im using a roblox wrapper; [Roboat](https://github.com/fekie/roboat) to achieve a more stable and better development with roblox's changes.
Credit to [rojo-rbx](https://github.com/rojo-rbx/rbx-dom) for making this program possible and easy to write.


For researching rust ive been using the official [rust book](https://doc.rust-lang.org/book/).

having AI (Claude) only help with ONLY the readme, lifetimes, and some refactors for optimization, as this is an educational project for me.


---

<div align="center">
   This is my first Rust project, I'm still learning Rust. So any contributions or suggestions will be accepted.

**⭐ If this project helped you, consider giving it a star! ⭐**

</div>

