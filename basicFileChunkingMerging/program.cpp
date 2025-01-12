#include <iostream>
#include <fstream>
#include <string>

using namespace std;

// Function to chunk a generic binary file into parts of size `chunkSize` bytes
void chunkFile(const string &fullFilePath, const string &chunkName, unsigned long chunkSize) {
    ifstream fileStream(fullFilePath, ios::in | ios::binary);

    if (fileStream.is_open()) {
        ofstream output;
        int counter = 1;

        string fullChunkName;
        string originalExtension;

        // Extract original file extension
        size_t dotPos = fullFilePath.find_last_of(".");
        if (dotPos != string::npos) {
            originalExtension = fullFilePath.substr(dotPos);
        }

        // Create a buffer to hold each chunk
        char *buffer = new char[chunkSize];

        // Keep reading until end of file
        while (!fileStream.eof()) {
            fullChunkName = chunkName + to_string(counter) + originalExtension;

            output.open(fullChunkName.c_str(), ios::out | ios::trunc | ios::binary);

            if (output.is_open()) {
                fileStream.read(buffer, chunkSize);
                output.write(buffer, fileStream.gcount());
                output.close();

                counter++;
            }
        }

        delete[] buffer;
        fileStream.close();
        cout << "Binary chunking complete! " << counter - 1 << " files created." << endl;
    } else {
        cout << "Error opening file!" << endl;
    }
}

// Function to join binary file chunks into a single file
void joinFile(const string &chunkName, const string &fileOutput, const string &extension) {
    ofstream outputFile(fileOutput, ios::out | ios::binary);

    if (!outputFile.is_open()) {
        cerr << "Error: Unable to open output file for writing." << endl;
        return;
    }

    unsigned int counter = 1;
    bool fileFound = true;
    string fileName;

    while (fileFound) {
        fileName = chunkName + to_string(counter) + extension;

        ifstream fileInput(fileName, ios::in | ios::binary);
        if (fileInput.is_open()) {
            fileFound = true;

            fileInput.seekg(0, ios::end);
            size_t fileSize = fileInput.tellg();
            fileInput.seekg(0, ios::beg);

            char *inputBuffer = new char[fileSize];
            fileInput.read(inputBuffer, fileSize);
            outputFile.write(inputBuffer, fileSize);

            delete[] inputBuffer;
            fileInput.close();
        } else {
            fileFound = false;
        }

        counter++;
    }

    outputFile.close();
    cout << "Binary file assembly complete!" << endl;
}

// Function to chunk a CSV file by lines, including the column headers in each chunk
void chunkCSV(const string &csvPath, const string &chunkPrefix, unsigned long chunkSize) {
    ifstream csvInput(csvPath);

    if (csvInput.is_open()) {
        ofstream output;
        unsigned long currentSize = 0;
        int counter = 1;
        string line;
        string chunkName;
        bool isHeaderWritten = false;
        string headerLine;

        // Read the first line for headers
        if (getline(csvInput, headerLine)) {
            // Save the header for reuse in every chunk
            while (getline(csvInput, line)) {
                // Start a new chunk if the current size exceeds the limit
                if (currentSize == 0 || currentSize + line.size() > chunkSize) {
                    if (output.is_open()) {
                        output.close();
                    }

                    // Create new chunk file
                    chunkName = chunkPrefix + to_string(counter) + ".csv";
                    output.open(chunkName.c_str(), ios::out | ios::trunc);

                    if (!output.is_open()) {
                        cerr << "Error: Unable to open chunk file for writing." << endl;
                        return;
                    }

                    // Write the header in each new chunk
                    output << headerLine << "\n";
                    counter++;
                    currentSize = 0;
                    isHeaderWritten = true;
                }

                // Write the current line to the chunk
                output << line << "\n";
                currentSize += line.size();
            }
        }

        if (output.is_open()) {
            output.close();
        }

        csvInput.close();
        cout << "CSV chunking complete! " << counter - 1 << " files created." << endl;
    } else {
        cerr << "Error: Unable to open CSV file!" << endl;
    }
}

void joinCSV(const string &chunkPrefix, const string &outputFile) {
    ofstream output(outputFile, ios::out | ios::trunc);

    if (!output.is_open()) {
        cerr << "Error: Unable to open output CSV file!" << endl;
        return;
    }

    int counter = 1;
    bool isFirstChunk = true; // Flag to track the first chunk
    string chunkName;
    string line;

    while (true) {
        chunkName = chunkPrefix + to_string(counter) + ".csv";

        ifstream chunkInput(chunkName);

        if (!chunkInput.is_open()) {
            break; // No more chunks found
        }

        // Read the first line (header or data)
        if (getline(chunkInput, line)) {
            if (isFirstChunk) {
                // Write the header for the first chunk
                output << line << "\n";
                isFirstChunk = false; // Ensure headers are not written again
            }
        }

        // Write the rest of the lines (data only)
        while (getline(chunkInput, line)) {
            output << line << "\n";
        }

        chunkInput.close();
        counter++;
    }

    output.close();
    cout << "CSV file assembly complete!" << endl;
}



//Main
int main() {
    string filePath;
    string chunkPrefix;
    unsigned long chunkSizeMB;

    // Input file path
    cout << "Enter the name of the file you want to chunk: ";
    getline(cin, filePath);

    // Input chunk prefix
    cout << "Enter the prefix for the chunk files: ";
    getline(cin, chunkPrefix);

    // Input chunk size in MB
    cout << "Enter the chunk size in megabytes (e.g., 1 for 1 MB): ";
    cin >> chunkSizeMB;

    // Convert chunk size from MB to bytes
    unsigned long chunkSize = chunkSizeMB * 1024 * 1024;

    // Extract the file extension
    size_t dotPos = filePath.find_last_of(".");
    string extension = (dotPos != string::npos) ? filePath.substr(dotPos) : "";

    // Check file extension and call the appropriate function
    if (extension == ".csv") {
        cout << "Chunking the CSV file..." << endl;
        chunkCSV(filePath, chunkPrefix, chunkSize);

        cout << "Joining the CSV file chunks..." << endl;
        string outputFileCSV = "output.csv"; // Output file name after merging
        joinCSV(chunkPrefix, outputFileCSV);
    } else if (extension == ".mp3" || extension == ".mp4" || extension == ".bin") {
        cout << "Chunking the binary file..." << endl;
        chunkFile(filePath, chunkPrefix, chunkSize);

        cout << "Joining the binary file chunks..." << endl;
        string outputFileBinary = "output" + extension; // Output file name after merging
        joinFile(chunkPrefix, outputFileBinary, extension);
    } else {
        cout << "Unsupported file type!" << endl;
    }

    return 0;
}
