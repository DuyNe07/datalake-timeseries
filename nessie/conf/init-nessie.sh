# Bổ sung vào phần cuối của init-nessie.sh
echo "Setting up Nessie modules directory..."
mkdir -p /nessie/modules

# Create modules directory structure
echo "Creating modules directory if it doesn't exist..."
mkdir -p /nessie/modules

# Copy module files from source to container
echo "Copying Nessie Python modules to container..."
cp -r /opt/nessie/modules/* /nessie/modules/ 2>/dev/null || echo "No source modules to copy"

# Create __init__.py for modules directory if it doesn't exist
if [ ! -f "/nessie/modules/__init__.py" ]; then
    echo '"""Nessie utility modules."""' > /nessie/modules/__init__.py
    echo "Created modules/__init__.py"
fi

# Create bin directory for executable scripts
echo "Creating bin directory for executable scripts..."
mkdir -p /nessie/bin

# Copy executable scripts if they exist
cp -r /opt/nessie/bin/* /nessie/bin/ 2>/dev/null || echo "No bin scripts to copy"

# Make scripts executable
if [ -d "/nessie/bin" ]; then
    chmod +x /nessie/bin/*.py 2>/dev/null || true
    echo "Made bin scripts executable"
fi

echo "Nessie modules setup completed!"