/*
 * Copyright 2014 Canonical Ltd.
 *
 * Authors:
 * Sergio Schvezov: sergio.schvezov@cannical.com
 *
 * This file is part of mms.
 *
 * mms is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 3.
 *
 * mms is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package mms

import (
	"errors"
	"fmt"
	"log"
	"time"

	"gitlab.com/ubports/development/core/go-ldm"
)

func (pdu *MNotificationInd) DownloadContent(proxyHost string, proxyPort int32) (string, error) {
	downloadManager, err := ldm.NewDownloadManager()
	if err != nil {
		return "", err
	}
	download, err := downloadManager.CreateMmsDownload(pdu.ContentLocation, proxyHost, proxyPort)
	if err != nil {
		return "", err
	}
	s := download.Started()
	pa := download.Paused()
	r := download.Resumed()
	c := download.Canceled()
	f := download.Finished()
	p := download.DownloadProgress()
	e := download.Error()
	log.Print("Starting download of ", pdu.ContentLocation, " with proxy ", proxyHost, ":", proxyPort)
	download.Start()
	for {
		select {
		case started := <-s:
			if !started {
				return "", errors.New("download fails to start???")
			} else {
				log.Print("Started.")
			}
		case progress := <-p:
			log.Print("Progress:", progress.Total, progress.Received)
		case downloadFilePath := <-f:
			log.Print("File downloaded to ", downloadFilePath)
			return downloadFilePath, nil
		case <-time.After(3 * time.Minute):
			return "", fmt.Errorf("Download timeout exceeded while fetching %s", pdu.ContentLocation)
		case err := <-e:
			return "", err

		// Workaround https://gitlab.com/ubports/development/core/go-ldm/-/issues/2
		// We don't really care about paused/resumed/canceled (because
		// we don't perform those action), but if we don't listen to
		// these signals and it's emitted then go-ldm will lock up.
		case <-pa:
		case <-r:
		case <-c:
		}
	}
}

func Upload(file, msc, proxyHost string, proxyPort int32) (string, error) {
	ldm, err := ldm.NewUploadManager()
	if err != nil {
		return "", err
	}
	upload, err := ldm.CreateMmsUpload(msc, file, proxyHost, proxyPort)
	if err != nil {
		return "", err
	}
	s := upload.Started()
	c := upload.Canceled()
	f := upload.Finished()
	p := upload.UploadProgress()
	e := upload.Error()
	log.Print("Starting upload of ", file, " to ", msc, " with proxy ", proxyHost, ":", proxyPort)
	if err := upload.Start(); err != nil {
		return "", err
	}

	for {
		select {
		case started := <-s:
			if !started {
				// XXX: cleanup?
				return "", errors.New("upload fails to start???")
			} else {
				log.Print("Started.")
			}
		case progress := <-p:
			log.Print("Progress:", progress.Total, progress.Received)
		case responseFile := <-f:
			log.Print("File ", responseFile, " returned in upload")
			return responseFile, nil
		case <-time.After(10 * time.Minute):
			return "", errors.New("upload timeout")
		case err := <-e:
			return "", err

		// Workaround https://gitlab.com/ubports/development/core/go-ldm/-/issues/2
		// We don't really care about canceled (because we don't perform
		// cancel), but if we don't listen to these signals and it's
		// emitted then go-ldm will lock up.
		case <-c:
		}
	}
}
