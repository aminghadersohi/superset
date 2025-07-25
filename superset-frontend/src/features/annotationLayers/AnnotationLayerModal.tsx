/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { FunctionComponent, useState, useEffect, ChangeEvent } from 'react';

import { styled, t } from '@superset-ui/core';
import { useSingleViewResource } from 'src/views/CRUD/hooks';
import { ModalTitleWithIcon } from 'src/components/ModalTitleWithIcon';
import { Typography } from '@superset-ui/core/components/Typography';
import { Input, Modal } from '@superset-ui/core/components';
import withToasts from 'src/components/MessageToasts/withToasts';

import { OnlyKeyWithType } from 'src/utils/types';
import { AnnotationLayerObject } from './types';

type AnnotationLayerObjectStringKeys = keyof Pick<
  AnnotationLayerObject,
  OnlyKeyWithType<AnnotationLayerObject, string>
>;

interface AnnotationLayerModalProps {
  addDangerToast: (msg: string) => void;
  addSuccessToast: (msg: string) => void;
  layer?: AnnotationLayerObject | null;
  onLayerAdd?: (layer?: AnnotationLayerObject) => void;
  onHide: () => void;
  show: boolean;
}

const StyledAnnotationLayerTitle = styled.div`
  margin: ${({ theme }) => theme.sizeUnit * 2}px auto
    ${({ theme }) => theme.sizeUnit * 4}px auto;
`;

const LayerContainer = styled.div`
  margin-bottom: ${({ theme }) => theme.sizeUnit * 10}px;

  .control-label {
    margin-bottom: ${({ theme }) => theme.sizeUnit * 2}px;
  }

  .required {
    margin-left: ${({ theme }) => theme.sizeUnit / 2}px;
    color: ${({ theme }) => theme.colorError};
  }

  textarea,
  input[type='text'] {
    padding: ${({ theme }) => theme.sizeUnit * 1.5}px
      ${({ theme }) => theme.sizeUnit * 2}px;
    border: 1px solid ${({ theme }) => theme.colorBorder};
    border-radius: ${({ theme }) => theme.borderRadius}px;
    width: 50%;
  }

  input,
  textarea {
    flex: 1 1 auto;
  }

  textarea {
    width: 100%;
    height: 160px;
    resize: none;
  }

  input::placeholder,
  textarea::placeholder {
    color: ${({ theme }) => theme.colors.grayscale.light1};
  }
`;

const AnnotationLayerModal: FunctionComponent<AnnotationLayerModalProps> = ({
  addDangerToast,
  addSuccessToast,
  onLayerAdd,
  onHide,
  show,
  layer = null,
}) => {
  const [disableSave, setDisableSave] = useState<boolean>(true);
  const [currentLayer, setCurrentLayer] =
    useState<AnnotationLayerObject | null>();
  const [isHidden, setIsHidden] = useState<boolean>(true);
  const isEditMode = layer !== null;

  // annotation layer fetch logic
  const {
    state: { loading, resource },
    fetchResource,
    createResource,
    updateResource,
  } = useSingleViewResource<AnnotationLayerObject>(
    'annotation_layer',
    t('annotation_layer'),
    addDangerToast,
  );

  const resetLayer = () => {
    // Reset layer
    setCurrentLayer({
      name: '',
      descr: '',
    });
  };

  // Functions
  const hide = () => {
    setIsHidden(true);

    // Reset layer
    resetLayer();

    onHide();
  };

  const onSave = () => {
    if (isEditMode) {
      // Edit
      if (currentLayer?.id) {
        const update_id = currentLayer.id;
        delete currentLayer.id;
        delete currentLayer.created_by;
        updateResource(update_id, currentLayer).then(response => {
          if (!response) {
            return;
          }

          hide();
          addSuccessToast(t('Annotation template updated'));
        });
      }
    } else if (currentLayer) {
      // Create
      createResource(currentLayer).then(response => {
        if (!response) {
          return;
        }

        if (onLayerAdd) {
          onLayerAdd(response);
        }

        hide();
        addSuccessToast(t('Annotation template created'));
      });
    }
  };

  const onTextChange = (
    event: ChangeEvent<HTMLTextAreaElement> | ChangeEvent<HTMLInputElement>,
  ) => {
    const { target } = event;
    const data = {
      ...currentLayer,
      name: currentLayer ? currentLayer.name : '',
      descr: currentLayer ? currentLayer.descr : '',
    };

    data[target.name as AnnotationLayerObjectStringKeys] = target.value;
    setCurrentLayer(data);
  };

  const validate = () => {
    if (currentLayer?.name?.length) {
      setDisableSave(false);
    } else {
      setDisableSave(true);
    }
  };

  // Initialize
  useEffect(() => {
    if (
      isEditMode &&
      (!currentLayer?.id ||
        (layer && layer.id !== currentLayer.id) ||
        (isHidden && show))
    ) {
      if (show && layer && layer.id !== null && !loading) {
        const id = layer.id || 0;

        fetchResource(id);
      }
    } else if (
      !isEditMode &&
      (!currentLayer || currentLayer.id || (isHidden && show))
    ) {
      // Reset layer
      resetLayer();
    }
  }, [layer, show]);

  useEffect(() => {
    if (resource) {
      setCurrentLayer(resource);
    }
  }, [resource]);

  // Validation
  useEffect(() => {
    validate();
  }, [
    currentLayer ? currentLayer.name : '',
    currentLayer ? currentLayer.descr : '',
  ]);

  // Show/hide
  if (isHidden && show) {
    setIsHidden(false);
  }

  return (
    <Modal
      disablePrimaryButton={disableSave}
      onHandledPrimaryAction={onSave}
      onHide={hide}
      primaryButtonName={isEditMode ? t('Save') : t('Add')}
      show={show}
      width="55%"
      name={
        isEditMode
          ? t('Edit annotation layer properties')
          : t('Add annotation layer')
      }
      title={
        <ModalTitleWithIcon
          isEditMode={isEditMode}
          title={
            isEditMode
              ? t('Edit annotation layer properties')
              : t('Add annotation layer')
          }
          data-test="annotation-layer-modal-title"
        />
      }
    >
      <StyledAnnotationLayerTitle>
        <Typography.Title level={4}>{t('Basic information')}</Typography.Title>
      </StyledAnnotationLayerTitle>
      <LayerContainer>
        <div className="control-label">
          {t('Annotation layer name')}
          <span className="required">*</span>
        </div>
        <Input
          name="name"
          onChange={onTextChange}
          type="text"
          value={currentLayer?.name}
        />
      </LayerContainer>
      <LayerContainer>
        <div className="control-label">{t('description')}</div>
        <Input.TextArea
          name="descr"
          value={currentLayer?.descr}
          placeholder={t('Description (this can be seen in the list)')}
          onChange={onTextChange}
        />
      </LayerContainer>
    </Modal>
  );
};

export default withToasts(AnnotationLayerModal);
