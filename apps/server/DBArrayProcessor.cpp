/* Copyright (C) 2014 Carlos Aguilar Melchor, Joris Barrier, Marc-Olivier Killijian
 * This file is part of XPIR.
 *
 *  XPIR is free software: you can redistribute it and/or modify
 *	it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  XPIR is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with XPIR.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "DBArrayProcessor.hpp"

// This constructor is called when we need File-splitting
DBArrayProcessor::DBArrayProcessor(uint64_t len, char *bytes, uint64_t nbStreams) : filesSplitting(true) {

	maxFileBytesize=0;

	if (bytes != NULL) 
	{
    bytesStream = bytes;
    lenStream = len;

		// Add File object on the file list
		std::string fileName="byteStream";
		realFileName=fileName;

		uint64_t realFileSize = len;
		maxFileBytesize = realFileSize/nbStreams;

		if(maxFileBytesize==0) {
			std::cout << "DBArrayProcessor: ERROR cannot split a stream en less than one byte elements!" << std::endl;
			std::cout << "DBArrayProcessor: file " << realFileName << " is only "<< realFileSize << " long" << std::endl;
			exit(1);
		}

		for(int i=0;i<nbStreams;i++) {
			file_list.push_back( std::to_string(i) );
		}
	}
	else // If there was a problem opening the directory
	{
		std::cout << "DBArrayProcessor: Passed a null pointer " << std::endl;
		exit(1);
	}

#ifdef DEBUG
	std::cout << "maxFileBytesize." <<maxFileBytesize<< std::endl;
	std::cout << "file_list.size()." <<file_list.size()<< std::endl;

#endif
	std::cout << "DBArrayProcessor: The size of the database is " << maxFileBytesize*file_list.size() << " bytes" << std::endl;
	std::cout << "DBArrayProcessor: The number of elements in the catalog is " << file_list.size() << std::endl;
}

DBArrayProcessor::~DBArrayProcessor() {
}

std::string DBArrayProcessor::getCatalog(const bool typeOfCatalog) {
	std::string buf;
	
  if(typeOfCatalog) {
		// Start with the number of elements in the catalog
		buf = std::to_string((unsigned int)0)+ "\n";	
		buf += std::to_string(getNbStream())+ "\n";		
		// Then for each file contactenate (with newlines) filename and filesize
		for (auto f : file_list)
		{
				buf += f + "\n" + std::to_string(getmaxFileBytesize()) + "\n";
		}
		return buf;
	} 
	// else we want a compact representation, i.e. nbFiles / fileSize
	buf = std::to_string((unsigned int)1)+ "\n";	
	buf += std::to_string(getNbStream())+ "\n";
	buf += std::to_string(maxFileBytesize)+ "\n";
	return buf;
}

uint64_t DBArrayProcessor::getDBSizeBits() {
	return maxFileBytesize*file_list.size()*8;
}
uint64_t DBArrayProcessor::getNbStream() {
	return  file_list.size();
}
uint64_t DBArrayProcessor::getmaxFileBytesize() {
	return maxFileBytesize;
}

std::ifstream* DBArrayProcessor::openStream(uint64_t streamNb, uint64_t requested_offset) {

	// When there is no splitting, each ifstream is associated with a real file 
	// (at least when no aggregation is done which is the case for now)
		// But when we are doing file splitting, we just need to position the ifstream at the correct position
		uint64_t splitting_offset=streamNb*getmaxFileBytesize();
		ArrayStream *s = new ArrayStream();
    s->stream = &bytesStream[splitting_offset+requested_offset];
    s->requested_offset = requested_offset;

    return (std::ifstream*) s;
}

uint64_t DBArrayProcessor::readStream(std::ifstream* is, char * buf, uint64_t size) {
  ArrayStream *s = (ArrayStream*) is;

  if (s->requested_offset > getmaxFileBytesize()) {
    bzero(buf, size);
    return size;
  }
	
  uint64_t max_bytes_to_read = getmaxFileBytesize() - s->requested_offset;
  
  uint64_t bytes_to_read;

  if (size < max_bytes_to_read) {
    bytes_to_read = size;
  } else {
    bytes_to_read = max_bytes_to_read;
  }
  
  std::memcpy(buf, s->stream, bytes_to_read);
  
  if (bytes_to_read < size) {
    bzero(buf+bytes_to_read, size-bytes_to_read);
  }

  return size;
}

void DBArrayProcessor::closeStream(std::ifstream* s) {
}

std::streampos DBArrayProcessor::getFileSize( std::string filePath ){
  return getmaxFileBytesize();
}

void DBArrayProcessor::readAggregatedStream(uint64_t streamNb, uint64_t alpha, uint64_t offset, uint64_t bytes_per_file, char* rawBits){
	uint64_t fileByteSize = std::min(bytes_per_file, getmaxFileBytesize()-offset);
	uint64_t startStream = streamNb*alpha;
	uint64_t endStream = std::min(streamNb*alpha + alpha - 1, getNbStream() - 1);
	uint64_t paddingStreams = (streamNb*alpha+alpha) >= getNbStream() ? (streamNb*alpha+alpha) - getNbStream() : 0;

  #pragma omp critical
	{	
		for (int i=startStream; i <= endStream; i++)
		{
			std::ifstream *stream = openStream(i, offset);

			// Just read the file (plus padding for that file)
			readStream(stream, rawBits + (i % alpha) * fileByteSize, fileByteSize);

			closeStream(stream);
		} 

		if(paddingStreams !=0)
		{
			bzero(rawBits + (endStream % alpha) * fileByteSize, fileByteSize*paddingStreams);
		}
	}
}
