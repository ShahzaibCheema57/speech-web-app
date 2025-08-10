from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import os
from pydub import AudioSegment
import io

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)  # Ensure uploads folder exists
TSV_FILE = "text_file_sample.tsv"

def get_next_text():
    df = pd.read_csv(TSV_FILE, sep="\t")
    df.columns = df.columns.str.strip()  # Ensure no extra spaces in column names
    
    # Find the next available row where 'recording' is not 'done'
    next_entries = df[df["recording"] != "done"]
    
    if next_entries.empty:
        return None  # No more available text

    return next_entries.iloc[0]

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        name = request.form.get("name")
        audio_file = request.files["audio"]
        form_chunk_id = request.form.get("chunk_id")
        


        if name and audio_file:
            df = pd.read_csv(TSV_FILE, sep="\t")
            df.columns = df.columns.str.strip()  # Clean column names
            
            
            # Find the first row where recording is NOT done
            #row_index = df[df["recording"] != "done"].index[0]
            print("form_chunk_id", form_chunk_id)
            #row_index = df.index[df["chunk_id"] == form_chunk_id].tolist()[0]#df[df["recording"] != "done"].index[0]

            text_entry = df.loc[df["chunk_id"] == str(form_chunk_id).strip(), "text"]

            # Save the uploaded file
            # Assuming `chunk_number` is a column in `df` corresponding to the row
            chunk_number = df.loc[df["chunk_id"] == form_chunk_id, "chunk_id"]  # Get chunk_number for the current row
            # Modify the filename to use chunk_number instead of row_index
            audio_filename = f"{name}_{chunk_number}.wav"
            audio_path = os.path.join(UPLOAD_FOLDER, audio_filename)
            # Read the uploaded file as bytes
            audio_bytes = audio_file.read()
            # Convert to WAV format using pydub
            audio = AudioSegment.from_file(io.BytesIO(audio_bytes))  # Auto-detect format
            audio = audio.set_channels(1).set_frame_rate(16000)  # Convert to mono 16kHz
            audio.export(audio_path, format="wav")  # Save as proper WAV
            # Update the TSV file
            print("before >> ",df.loc[df["chunk_id"] == form_chunk_id, "recording"])
            df.loc[df["chunk_id"] == form_chunk_id, "recording"] = "done"
            df.loc[df["chunk_id"] == form_chunk_id, "audio path"] = audio_path
            df.loc[df["chunk_id"] == form_chunk_id, "recorder"] = name
            print("after >> ",df.loc[df["chunk_id"] == form_chunk_id, "recording"])

            # Save back to TSV
            df.to_csv(TSV_FILE, sep="\t", index=False)

            print("DEBUG - Updated DataFrame:\n", df.head())  # Print to verify

            return redirect(url_for("index"))  # Refresh to show next text

    entry = get_next_text()
    print("entry >> ",entry)
    if entry is None:
        return "All recordings are done!"

    return render_template("index.html", text=entry["text"],chunk_id=entry["chunk_id"])

if __name__ == "__main__":
    app.run(debug=True,ssl_context=('cert.pem', 'key.pem'), host="0.0.0.0", port=443)
    
