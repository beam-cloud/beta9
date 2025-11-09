#!/usr/bin/env python3
"""
Demo: Docker Compose with gVisor

This script demonstrates using docker-compose in a gVisor sandbox,
which now works correctly with the upload_file fix.

The fix ensures that override files uploaded via fs.upload_file()
are visible to docker-compose running via process.exec().
"""

from beta9 import Image, Sandbox

def main():
    print("🚀 Creating sandbox with Docker enabled...")
    sandbox = Sandbox(
        docker_enabled=True,
        image=Image().with_docker()
    ).create()
    
    try:
        print("✓ Sandbox created:", sandbox.container_id)
        
        # Create a simple docker-compose file
        compose_content = """services:
  redis:
    image: redis:alpine
    ports:
      - '6379:6379'
  web:
    image: nginx:alpine
    ports:
      - '8000:80'
    depends_on:
      - redis
"""
        
        print("\n📝 Creating docker-compose.yml in sandbox...")
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as tmp:
            tmp.write(compose_content)
            local_path = tmp.name
        
        try:
            sandbox.fs.upload_file(local_path, "/workspace/docker-compose.yml")
            print("✓ Uploaded docker-compose.yml")
        finally:
            os.unlink(local_path)
        
        # Verify the file is readable
        print("\n🔍 Verifying file is readable...")
        p = sandbox.process.exec("cat", "/workspace/docker-compose.yml", cwd="/workspace")
        p.wait()
        if p.exit_code == 0:
            print("✓ File is readable by exec commands")
        else:
            print("✗ Failed to read file:", p.stderr())
            return
        
        # Use the SDK's compose_up method which generates override for gVisor
        print("\n🐳 Starting Docker Compose with gVisor networking...")
        print("   (Automatically applying network_mode: host for each service)")
        
        process = sandbox.docker.compose_up(
            file="docker-compose.yml",
            detach=True,
            build=False,
            cwd="/workspace"
        )
        
        print("\n📊 Docker Compose output:")
        print(process.stdout())
        
        if process.exit_code == 0:
            print("\n✅ Success! Docker Compose is running with gVisor")
            print("   Services are using host networking (compatible with gVisor)")
            
            # List running containers
            print("\n📦 Running containers:")
            p = sandbox.process.exec("docker", "ps", cwd="/workspace")
            p.wait()
            print(process.stdout())
            
        else:
            print("\n❌ Docker Compose failed:")
            print(process.stderr())
        
    finally:
        print("\n🧹 Cleaning up...")
        sandbox.terminate()
        print("✓ Sandbox terminated")

if __name__ == "__main__":
    main()
